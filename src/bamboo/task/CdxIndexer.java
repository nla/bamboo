package bamboo.task;

import bamboo.core.*;
import bamboo.crawl.*;
import bamboo.crawl.Collection;
import bamboo.crawl.Collections;
import bamboo.util.SurtFilter;
import org.apache.jute.Record;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

import static bamboo.task.WarcUtils.cleanUrl;

public class CdxIndexer implements Runnable {
    private static final int BATCH_SIZE = 1024;
    private final Warcs warcs;
    private final Crawls crawls;
    private final Serieses serieses;
    private final Collections collections;
    private final List<Consumer<Long>> warcIndexedListeners = new ArrayList<>();

    public CdxIndexer(Warcs warcs, Crawls crawls, Serieses serieses, Collections collections) {
        this.warcs = warcs;
        this.crawls = crawls;
        this.serieses = serieses;
        this.collections = collections;
    }

    public void onWarcIndexed(Consumer<Long> callback) {
        warcIndexedListeners.add(callback);
    }

    public void run() {
        while (true) {
            List<Warc> candidates = warcs.findByState(Warc.IMPORTED, BATCH_SIZE);

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

        // fetch the list of collections from the database
        List<CdxBuffer> buffers = new ArrayList<>();
        Crawl crawl = crawls.get(warc.getCrawlId());
        for (CollectionWithFilters collection: collections.findByCrawlSeriesId(crawl.getCrawlSeriesId())) {
            buffers.add(new CdxBuffer(collection));
        }

        // parse the warc file
        RecordStats stats;
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
        Map<Long,RecordStats> collectionStats = new HashMap<>();
        for (CdxBuffer buffer: buffers) {
            buffer.submit();
            collectionStats.put(buffer.collection.getId(), buffer.stats);
        }

        // update the statistics in the database
        warcs.updateRecordStats(warc.getId(), stats);
        warcs.updateCollections(warc.getId(), collectionStats);

        // mark indexing as finished
        warcs.updateState(warc.getId(), Warc.CDX_INDEXED);

        System.out.println("Finished CDX indexing " + warc.getId() + " " + warc.getPath() + " " + stats);
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

    public static class CdxBuffer {
        final Collection collection;
        final URL cdxServer;
        final SurtFilter filter;
        final Writer buf;
        final RecordStats stats = new RecordStats();

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

    final static Pattern schemeRegex = Pattern.compile("^[a-zA-Z][a-zA-Z+.-]*://?");

    static String stripScheme(String surt) {
        return schemeRegex.matcher(surt).replaceFirst("");
    }

    static String toSchemalessSURT(String url) {
        return SURT.toSURT(stripScheme(url));
    }

    private static RecordStats writeCdx(Path warc, String filename, List<CdxBuffer> buffers) throws IOException {
        RecordStats stats = new RecordStats();
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
