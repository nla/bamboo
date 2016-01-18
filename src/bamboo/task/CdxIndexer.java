package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import bamboo.util.SurtFilter;
import org.archive.io.ArchiveReader;
import org.archive.url.SURT;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

import static bamboo.task.Warcs.cleanUrl;

public class CdxIndexer implements Runnable {
    private static final int BATCH_SIZE = 1024;
    private final DbPool dbPool;
    private final List<Consumer<Long>> warcIndexedListeners = new ArrayList<>();

    public CdxIndexer(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public void onWarcIndexed(Consumer<Long> callback) {
        warcIndexedListeners.add(callback);
    }

    public void run() {
        while (true) {
            List<Db.Warc> warcs;

            try (Db db = dbPool.take()) {
                warcs = db.findWarcsInState(Db.Warc.IMPORTED, BATCH_SIZE);
            }

            if (warcs.isEmpty()) {
                break;
            }

            indexWarcs(warcs);
        }
    }

    public void indexWarcs(List<Db.Warc> warcs) {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        try {
            for (Db.Warc warc : warcs) {
                threadPool.submit(() -> {
                    try {
                        indexWarc(warc);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                });
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            threadPool.shutdownNow();
        }
    }

    private void indexWarc(Db.Warc warc) throws IOException {
        System.out.println("\nCDX indexing " + warc.id + " " + warc.path);
        Db.Crawl crawl;
        List<CdxBuffer> buffers = new ArrayList<>();

        // fetch the list of collections from the database
        try (Db db = dbPool.take()) {
            crawl = db.findCrawl(warc.crawlId);
            for (Db.CollectionWithFilters collection: db.listCollectionsForCrawlSeries(crawl.crawlSeriesId)) {
                buffers.add(new CdxBuffer(collection));
            }
        }

        // parse the warc file
        Stats stats;
        try {
            stats = writeCdx(warc.path, warc.filename, buffers);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof ZipException) {
                try (Db db = dbPool.take()) {
                    db.updateWarcState(warc.id, Db.Warc.CDX_ERROR);
                    return;
                }
            } else {
                throw e;
            }
        } catch (ZipException | FileNotFoundException e) {
            try (Db db = dbPool.take()) {
                db.updateWarcState(warc.id, Db.Warc.CDX_ERROR);
                return;
            }
        } catch (IOException e) {
            if (e.getMessage().endsWith(" is not a WARC file.")) {
                try (Db db = dbPool.take()) {
                    db.updateWarcState(warc.id, Db.Warc.CDX_ERROR);
                    return;
                }
            } else {
                throw e;
            }
        }

        // submit the records to each collection
        for (CdxBuffer buffer: buffers) {
            buffer.submit();
        }

        // update the records and statistics in the database
        try (Db db = dbPool.take()) {
            db.inTransaction((t, s) -> {
                db.updateWarcStateWithoutHistory(warc.id, Db.Warc.CDX_INDEXED);
                db.insertWarcHistory(warc.id, Db.Warc.CDX_INDEXED);
                db.updateWarcRecordStats(warc.id, stats.records, stats.bytes);

                // we subtract the original counts to prevent doubling up the stats when reindexing
                // if this is a straight reindex with no changes these deltas will be zero
                long warcRecordsDelta = stats.records - warc.records;
                long warcBytesDelta = stats.bytes - warc.recordBytes;

                db.incrementRecordStatsForCrawl(warc.crawlId, warcRecordsDelta, warcBytesDelta);

                if (crawl.crawlSeriesId != null) {
                    db.incrementRecordStatsForCrawlSeries(crawl.crawlSeriesId, warcRecordsDelta, warcBytesDelta);
                }

                if (stats.startTime != null) {
                    db.conditionallyUpdateCrawlStartTime(warc.crawlId, stats.startTime);
                }
                if (stats.endTime != null) {
                    db.conditionallyUpdateCrawlEndTime(warc.crawlId, stats.endTime);
                }

                // update each of the per-collection stats
                for (CdxBuffer buffer : buffers) {
                    long recordsDelta = buffer.stats.records;
                    long bytesDelta = buffer.stats.bytes;

                    Db.CollectionWarc old = db.findCollectionWarc(buffer.collection.id, warc.id);
                    if (old != null) {
                        recordsDelta -= old.records;
                        bytesDelta -= old.recordBytes;
                    }

                    db.deleteCollectionWarc(buffer.collection.id, warc.id);
                    db.insertCollectionWarc(buffer.collection.id, warc.id, buffer.stats.records, buffer.stats.bytes);
                    db.incrementRecordStatsForCollection(buffer.collection.id, recordsDelta, bytesDelta);
                }
                return null;
            });
        }
        System.out.println("Finished CDX indexing " + warc.id + " " + warc.path + " (" + stats.records + " records with " + stats.bytes + " bytes)");
        sendWarcIndexedNotification(warc.id);
    }

    private void sendWarcIndexedNotification(long warcId) {
        for (Consumer<Long> listener : warcIndexedListeners) {
            listener.accept(warcId);
        }
    }

    void indexWarc(long warcId) throws IOException {
        Db.Warc warc;
        try (Db db = dbPool.take()) {
            warc = db.findWarc(warcId);
        }
        indexWarc(warc);
    }

    private static class CdxBuffer {
        final Db.Collection collection;
        final URL cdxServer;
        final SurtFilter filter;
        final Writer buf;
        final Stats stats = new Stats();

        CdxBuffer(Db.CollectionWithFilters collection) throws MalformedURLException {
            this.collection = collection;
            cdxServer = new URL(collection.cdxUrl);
            filter = new SurtFilter(collection.urlFilters);
            buf = new StringWriter().append(" CDX N b a m s k r M S V g\n");
        }

        private CdxBuffer(Writer buf) {
            collection = null;
            cdxServer = null;
            filter = null;
            this.buf = buf;
        }

        void append(String surt, long recordLength, String cdxLine, Date time) {
            if (filter == null || filter.accepts(surt)) {
                try {
                    buf.write(cdxLine + "\n");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                stats.update(recordLength, time);
            }
        }

        void submit() throws IOException {
            byte[] data = buf.toString().getBytes(StandardCharsets.UTF_8);
            HttpURLConnection conn = (HttpURLConnection) cdxServer.openConnection();
            conn.setRequestMethod("POST");
            conn.addRequestProperty("Content-Type", "text/plain");
            conn.setFixedLengthStreamingMode(data.length);
            conn.setDoOutput(true);

            try (OutputStream out = conn.getOutputStream()) {
                out.write(data);
                out.flush();
            }

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String output = rdr.readLine();
                int status = conn.getResponseCode();
                if (status != 200) {
                    throw new RuntimeException("Indexing failed: " + output);
                }
            }
        }

        void appendAlias(String alias, String target) {
            String surt = toSchemalessSURT(alias);
            if (filter == null || filter.accepts(surt)) {
                try {
                    buf.write("@alias ");
                    buf.write(cleanUrl(alias));
                    buf.write(' ');
                    buf.write(cleanUrl(target));
                    buf.write('\n');
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class Stats {
        long records = 0;
        long bytes = 0;
        Date startTime = null;
        Date endTime = null;

        void update(long recordLength, Date time) {
            records += 1;
            bytes += recordLength;

            if (startTime == null || time.before(startTime)) {
                startTime = time;
            }

            if (endTime == null || time.after(endTime)) {
                endTime = time;
            }
        }
    }

    final static Pattern schemeRegex = Pattern.compile("^[a-zA-Z][a-zA-Z+.-]*://?");

    static String stripScheme(String surt) {
        return schemeRegex.matcher(surt).replaceFirst("");
    }

    static String toSchemalessSURT(String url) {
        return SURT.toSURT(stripScheme(url));
    }

    private static Stats writeCdx(Path warc, String filename, List<CdxBuffer> buffers) throws IOException {
        Stats stats = new Stats();
        try (ArchiveReader reader = Warcs.open(warc)) {
            Cdx.records(reader, filename).forEach(record -> {
                if (record instanceof Cdx.Alias) {
                    Cdx.Alias alias = (Cdx.Alias) record;
                    for (CdxBuffer buffer : buffers) {
                        buffer.appendAlias(alias.alias, alias.target);
                    }
                } else {
                    Cdx.Capture capture = (Cdx.Capture) record;
                    Date time;
                    try {
                        time = Warcs.parseArcDate(capture.date);
                    } catch (DateTimeParseException e) {
                        return; // skip record if we can't get a sane time
                    }
                    stats.update(capture.contentLength, time);
                    String surt = toSchemalessSURT(capture.url);
                    for (CdxBuffer buffer : buffers) {
                        buffer.append(surt, capture.contentLength, capture.toCdxLine(), time);
                    }
                }
            });
        }
        return stats;
    }

    public static void main(String args[]) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: CdxIndexer warc");
            System.exit(1);
        }
        Path warc = Paths.get(args[0]);
        Cdx.writeCdx(warc, warc.getFileName().toString(), new OutputStreamWriter(System.out));
    }

}
