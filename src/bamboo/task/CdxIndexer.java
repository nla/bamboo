package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import bamboo.util.SurtFilter;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.url.SURT;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.ZipException;

public class CdxIndexer implements Runnable {
    final private DbPool dbPool;

    public CdxIndexer(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public void run() {
        while (true) {
            List<Db.Warc> warcs;

            try (Db db = dbPool.take()) {
                warcs = db.findWarcsToCdxIndex();
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
            stats = writeCdx(warc.path, buffers);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof ZipException) {
                try (Db db = dbPool.take()) {
                    db.updateWarcCorrupt(warc.id, Db.GZIP_CORRUPT);
                    return;
                }
            } else {
                throw e;
            }
        } catch (ZipException e) {
            try (Db db = dbPool.take()) {
                db.updateWarcCorrupt(warc.id, Db.GZIP_CORRUPT);
                return;
            }
        } catch (FileNotFoundException e) {
            try (Db db = dbPool.take()) {
                db.updateWarcCorrupt(warc.id, Db.WARC_MISSING);
                return;
            }
        } catch (IOException e) {
            if (e.getMessage().endsWith(" is not a WARC file.")) {
                try (Db db = dbPool.take()) {
                    db.updateWarcCorrupt(warc.id, Db.WARC_CORRUPT);
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

        // update the database
        try (Db db = dbPool.take()) {
            db.inTransaction((t, s) -> {
                db.updateWarcCdxIndexed(warc.id, System.currentTimeMillis(), stats.records, stats.bytes);
                db.incrementRecordStatsForCrawl(warc.crawlId, stats.records, stats.bytes);
                if (crawl.crawlSeriesId != null) {
                    db.incrementRecordStatsForCrawlSeries(crawl.crawlSeriesId, stats.records, stats.bytes);
                }
                for (CdxBuffer buffer: buffers) {
                    db.incrementRecordStatsForCollection(buffer.collection.id, buffer.stats.records, buffer.stats.bytes);
                }
                return null;
            });
        }
        System.out.println("Finished CDX indexing " + warc.id + " " + warc.path + " (" + stats.records + " records with " + stats.bytes + " bytes)");
    }

    private static class CdxBuffer {
        final Db.Collection collection;
        final URL cdxServer;
        final SurtFilter filter;
        final StringWriter buf = new StringWriter().append(" CDX N b a m s k r M S V g\n");
        final Stats stats = new Stats();

        CdxBuffer(Db.CollectionWithFilters collection) throws MalformedURLException {
            this.collection = collection;
            cdxServer = new URL(collection.cdxUrl);
            filter = new SurtFilter(collection.urlFilters);
        }

        void append(String surt, long recordLength, String cdxLine) {
            if (filter.accepts(surt)) {
                buf.append(cdxLine);
                stats.update(recordLength);
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
    }

    private static class Stats {
        long records = 0;
        long bytes = 0;

        void update(long recordLength) {
            records += 1;
            bytes += recordLength;
        }
    }

    private static Stats writeCdx(Path warc, List<CdxBuffer> buffers) throws IOException {
        Stats stats = new Stats();
        String filename = warc.getFileName().toString();
        try (ArchiveReader reader = ArchiveReaderFactory.get(warc.toFile())) {
            for (ArchiveRecord record : reader) {
                String cdxLine = formatCdxLine(filename, record);
                if (cdxLine != null) {
                    long recordLength = record.getHeader().getContentLength();
                    stats.update(recordLength);
                    String surt = SURT.toSURT(Warcs.getCleanUrl(record.getHeader()));
                    for (CdxBuffer buffer : buffers) {
                        buffer.append(surt, recordLength, cdxLine);
                    }
                }
            }
        }
        return stats;
    }

    private static String formatCdxLine(String filename, ArchiveRecord record) throws IOException {
        ArchiveRecordHeader h = record.getHeader();
        if (!Warcs.isResponseRecord(h)) {
            return null;
        }
        String url = Warcs.getCleanUrl(h);
        HttpHeader http = HttpHeader.parse(record, url);
        if (http == null) {
            return null;
        }

        StringBuilder out = new StringBuilder();
        out.append('-').append(' '); // let server do canonicalization
        out.append(Warcs.getArcDate(h)).append(' ');
        out.append(url).append(' ');
        out.append(optional(http.getCleanContentType())).append(' ');
        out.append(http.status == -1 ? "-" : Integer.toString(http.status)).append(' ');
        out.append(optional(Warcs.getOrCalcDigest(record))).append(' ');
        out.append(optional(http.location)).append(' ');
        out.append("- "); // TODO: X-Robots-Tag http://noarchive.net/xrobots/
        out.append(Long.toString(h.getContentLength())).append(' ');
        out.append(Long.toString(h.getOffset())).append(' ');
        out.append(filename).append('\n');
        return out.toString();
    }

    private static String optional(String s) {
        if (s == null) {
            return "-";
        }
        return s.replace(" ", "%20").replace("\n", "%0A").replace("\r", "%0D");
    }

}
