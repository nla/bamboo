package bamboo.task;

import bamboo.core.*;
import bamboo.crawl.*;
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

import static bamboo.task.WarcUtils.cleanUrl;

public class CdxIndexer implements Runnable {
    private static final int BATCH_SIZE = 1024;
    private Warcs warcs;
    private Crawls crawls;
    private Serieses serieses;
    private Collections collections;
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
            List<Warc> candidates;

            try (Db db = dbPool.take()) {
                candidates = warcs.findByState(Warc.IMPORTED, BATCH_SIZE);
            }

            if (candidates.isEmpty()) {
                break;
            }

            indexWarcs(candidates);
        }
    }

    public void indexWarcs(List<Warc> candidates) {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        try {
            for (Warc warc : candidates) {
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

    private void indexWarc(Warc warc) throws IOException {
        System.out.println("\nCDX indexing " + warc.getId() + " " + warc.getPath());
        Crawl crawl;
        List<CdxBuffer> buffers = new ArrayList<>();

        // fetch the list of collections from the database
        try (Db db = dbPool.take()) {
            crawl = crawls.get(warc.getCrawlId());
            for (CollectionWithFilters collection: collections.findByCrawlSeriesId(crawl.getCrawlSeriesId())) {
                buffers.add(new CdxBuffer(collection));
            }
        }

        // parse the warc file
        Stats stats;
        try {
            stats = writeCdx(warc.getPath(), warc.getFilename(), buffers);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof ZipException) {
                warcs.updateState(warc.getId(), Warc.CDX_ERROR);
                return;
            } else {
                throw e;
            }
        } catch (ZipException | FileNotFoundException e) {
            warcs.updateState(warc.getId(), Warc.CDX_ERROR);
            return;
        } catch (IOException e) {
            if (e.getMessage().endsWith(" is not a WARC file.")) {
                warcs.updateState(warc.getId(), Warc.CDX_ERROR);
                return;
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
                db.updateWarcStateWithoutHistory(warc.getId(), Warc.CDX_INDEXED);
                db.insertWarcHistory(warc.getId(), Warc.CDX_INDEXED);
                db.updateWarcRecordStats(warc.getId(), stats.records, stats.bytes);

                // we subtract the original counts to prevent doubling up the stats when reindexing
                // if this is a straight reindex with no changes these deltas will be zero
                long warcRecordsDelta = stats.records - warc.getRecords();
                long warcBytesDelta = stats.bytes - warc.getRecordBytes();

                db.incrementRecordStatsForCrawl(warc.getCrawlId(), warcRecordsDelta, warcBytesDelta);

                if (crawl.getCrawlSeriesId() != null) {
                    db.incrementRecordStatsForCrawlSeries(crawl.getCrawlSeriesId(), warcRecordsDelta, warcBytesDelta);
                }

                if (stats.startTime != null) {
                    db.conditionallyUpdateCrawlStartTime(warc.getCrawlId(), stats.startTime);
                }
                if (stats.endTime != null) {
                    db.conditionallyUpdateCrawlEndTime(warc.getCrawlId(), stats.endTime);
                }

                // update each of the per-collection stats
                for (CdxBuffer buffer : buffers) {
                    long recordsDelta = buffer.stats.records;
                    long bytesDelta = buffer.stats.bytes;

                    Db.CollectionWarc old = db.findCollectionWarc(buffer.collection.getId(), warc.getId());
                    if (old != null) {
                        recordsDelta -= old.records;
                        bytesDelta -= old.recordBytes;
                    }

                    db.deleteCollectionWarc(buffer.collection.getId(), warc.getId());
                    db.insertCollectionWarc(buffer.collection.getId(), warc.getId(), buffer.stats.records, buffer.stats.bytes);
                    db.incrementRecordStatsForCollection(buffer.collection.getId(), recordsDelta, bytesDelta);
                }
                return null;
            });
        }
        System.out.println("Finished CDX indexing " + warc.getId() + " " + warc.getPath() + " (" + stats.records + " records with " + stats.bytes + " bytes)");
        sendWarcIndexedNotification(warc.getId());
    }

    private void sendWarcIndexedNotification(long warcId) {
        for (Consumer<Long> listener : warcIndexedListeners) {
            listener.accept(warcId);
        }
    }

    void indexWarc(long warcId) throws IOException {
        indexWarc(warcs.get(warcId));
    }

    private static class CdxBuffer {
        final Collection collection;
        final URL cdxServer;
        final SurtFilter filter;
        final Writer buf;
        final Stats stats = new Stats();

        CdxBuffer(CollectionWithFilters collection) throws MalformedURLException {
            this.collection = collection;
            cdxServer = new URL(collection.getCdxUrl());
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
        try (ArchiveReader reader = WarcUtils.open(warc)) {
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
                        time = WarcUtils.parseArcDate(capture.date);
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
