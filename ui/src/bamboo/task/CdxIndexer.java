package bamboo.task;

import bamboo.core.LockManager;
import bamboo.crawl.Collection;
import bamboo.crawl.*;
import bamboo.crawl.Collections;
import bamboo.util.Oidc;
import bamboo.util.SurtFilter;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.url.SURT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

import static bamboo.task.WarcUtils.cleanUrl;
import static java.nio.file.StandardOpenOption.*;

public class CdxIndexer implements Runnable {
    private static final Date YEAR1990 = new Date(631152000L);

    private static final Logger log = LoggerFactory.getLogger(CdxIndexer.class);
    private static final int BATCH_SIZE = 1024;
    private final Warcs warcs;
    private final Crawls crawls;
    private final Collections collections;
    private final LockManager lockManager;
    private final Oidc oidc;
    private final int threads;

    public CdxIndexer(Warcs warcs, Crawls crawls, Collections collections, LockManager lockManager,
                      Oidc oidc, int threads) {
        this.warcs = warcs;
        this.crawls = crawls;
        this.collections = collections;
        this.lockManager = lockManager;
        this.oidc = oidc;
        this.threads = threads;
    }

    public void run() {
        List<Warc> candidates = warcs.findByState(Warc.IMPORTED, BATCH_SIZE);
        if (!candidates.isEmpty()) {
            indexWarcs(candidates);
        }
    }

    private void indexWarcs(List<Warc> candidates) {
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        try {
            for (Warc warc : candidates) {
                threadPool.submit(() -> {
                    try {
                        String lockName = "warc-" + warc.getId();
                        if (lockManager.takeLock(lockName)) {
                            try {
                                indexWarc(warc);
                            } finally {
                                lockManager.releaseLock(lockName);
                            }
                        } else {
                            // warc is locked by someone else, skip it for now.
                        }
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

    public RecordStats indexWarc(Warc warc) throws IOException {
        System.out.println("\nCDX indexing " + warc.getId() + " " + warc.getPath());

        // fetch the list of collections from the database
        List<CdxBuffer> buffers = new ArrayList<>();
        Crawl crawl = crawls.get(warc.getCrawlId());
        for (CollectionWithFilters collection: collections.findByCrawlSeriesId(crawl.getCrawlSeriesId())) {
            if (collection.getCdxUrl() != null && !collection.getCdxUrl().isEmpty()) {
                buffers.add(new CdxBuffer(collection));
            }
        }

        RecordStats stats;
        Map<Long, RecordStats> collectionStats = new HashMap<>();

        try {
            // parse the warc file
            try (ArchiveReader reader = warcs.openReader(warc)){
                stats = writeCdx(reader, warc.getFilename(), warc.getSize(), buffers);
            } catch (RuntimeException e) {
                if (e.getCause() != null && e.getCause() instanceof ZipException) {
                    warcs.updateState(warc.getId(), Warc.CDX_ERROR);
                    return null;
                } else {
                    throw e;
                }
            } catch (ZipException | FileNotFoundException e) {
                warcs.updateState(warc.getId(), Warc.CDX_ERROR);
                return null;
            } catch (IOException e) {
                String message = e.getMessage();
                if (message != null && message.endsWith(" is not a WARC file.")) {
                    warcs.updateState(warc.getId(), Warc.CDX_ERROR);
                    return null;
                } else {
                    e.printStackTrace();
                    warcs.updateState(warc.getId(), Warc.CDX_ERROR);
                    return null;
                }
            }

            // submit the records to each collection
            for (CdxBuffer buffer : buffers) {
                buffer.submit();
                collectionStats.put(buffer.collection.getId(), buffer.stats);
            }
        } finally {
            for (CdxBuffer buffer: buffers) {
                buffer.close();
            }
        }


        // update the statistics in the database
        warcs.updateRecordStats(warc.getId(), stats);
        warcs.updateCollections(warc.getId(), collectionStats);

        // mark indexing as finished
        warcs.updateState(warc.getId(), Warc.CDX_INDEXED);

        System.out.println("Finished CDX indexing " + warc.getId() + " " + warc.getPath() + " " + stats);
        return stats;
    }

    void indexWarc(long warcId) throws IOException {
        indexWarc(warcs.get(warcId));
    }

    public boolean healthcheck(PrintWriter out) {
        boolean ok = true;

        out.print("Checking CDX indexes are reachable... ");

        for (Collection collection : collections.listAll()) {
            String url = collection.getCdxUrl();
            if (url != null && !url.isEmpty()) {
                String ref = "collection/" + collection.getId() + " " + url;
                try {
                    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                    if (conn.getResponseCode() != 200) {
                        out.println("ERROR: " + conn.getResponseCode() + " " + ref);
                        ok = false;
                    }
                    conn.disconnect();
                } catch (IOException e) {
                    out.println("ERROR: " + e.getMessage() + " " + ref);
                    e.printStackTrace(out);
                    ok = false;
                }
            }
        }

        if (ok) {
            out.println("OK");
        }
        return ok;
    }

    private class CdxBuffer implements Closeable {
        final Collection collection;
        final URL cdxServer;
        final SurtFilter filter;
        final Writer writer;
        final RecordStats stats = new RecordStats();
        private final Path bufferPath;
        private final FileChannel channel;

        CdxBuffer(CollectionWithFilters collection) throws IOException {
            this.collection = collection;
            cdxServer = new URL(collection.getCdxUrl());
            filter = new SurtFilter(collection.urlFilters);

            bufferPath = Files.createTempFile("bamboo", ".cdx");

            channel = FileChannel.open(bufferPath, DELETE_ON_CLOSE, READ, WRITE);

            writer = new BufferedWriter(Channels.newWriter(channel, "utf-8"));
            writer.append(" CDX N b a m s k r M S V g\n");
        }

        void append(String surt, long recordLength, String cdxLine, Date time) {
            if (filter == null || filter.accepts(surt)) {
                try {
                    writer.write(cdxLine + "\n");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                stats.update(recordLength, time);
            }
        }

        void submit() throws IOException {
            writer.flush();
            channel.position(0);

            HttpURLConnection conn = (HttpURLConnection) cdxServer.openConnection();
            conn.setRequestMethod("POST");
            conn.addRequestProperty("Content-Type", "text/plain");
            if (oidc != null) {
                conn.addRequestProperty("Authorization", oidc.accessToken().toAuthorizationHeader());
            }
            conn.setFixedLengthStreamingMode(channel.size());
            conn.setDoOutput(true);

            try (OutputStream out = conn.getOutputStream();
                 InputStream stream = Channels.newInputStream(channel)) {
                byte[] buf = new byte[8192];
                while (true) {
                    int n = stream.read(buf);
                    if (n < 0) {
                        break;
                    }
                    out.write(buf, 0, n);
                }

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
                    writer.write("@alias ");
                    writer.write(cleanUrl(alias));
                    writer.write(' ');
                    writer.write(cleanUrl(target));
                    writer.write('\n');
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    private final static Pattern schemeRegex = Pattern.compile("^[a-zA-Z][a-zA-Z+.-]*://?");

    static String stripScheme(String surt) {
        return schemeRegex.matcher(surt).replaceFirst("");
    }

    static String toSchemalessSURT(String url) {
        return SURT.toSURT(stripScheme(url));
    }

    private static RecordStats writeCdx(ArchiveReader reader, String filename, long warcLength, List<CdxBuffer> buffers) throws IOException {
        RecordStats stats = new RecordStats();
        Cdx.records(reader, filename, warcLength).forEach(record -> {
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
                    log.warn("Unable to parse time " + filename + " in " + capture.url, e);
                    return; // skip record if we can't get a sane time
                }
                if (time.before(YEAR1990)) {
                    return; // garbage. skip.
                }
                stats.update(capture.compressedLength, time);
                String surt = toSchemalessSURT(capture.url);
                for (CdxBuffer buffer : buffers) {
                    buffer.append(surt, capture.compressedLength, capture.toCdxLine(), time);
                }
            }
        });
        return stats;
    }

    public static void main(String args[]) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: CdxIndexer warc");
            System.exit(1);
        }
        Path warc = Paths.get(args[0]);
        Cdx.writeCdx(ArchiveReaderFactory.get(warc.toFile()), warc.getFileName().toString(), Files.size(warc),
                new OutputStreamWriter(System.out));
    }

}
