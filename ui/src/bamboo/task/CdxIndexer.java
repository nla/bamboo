package bamboo.task;

import bamboo.core.LockManager;
import bamboo.crawl.Collection;
import bamboo.crawl.*;
import bamboo.crawl.Collections;
import bamboo.util.Oidc;
import org.netpreserve.jwarc.WarcReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

public class CdxIndexer implements Runnable {
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
        return indexWarc(warc, false);
    }

    public RecordStats deindexWarc(Warc warc) throws IOException {
        return indexWarc(warc, true);
    }

    private RecordStats indexWarc(Warc warc, boolean deleteMode) throws IOException {
        System.out.println("\nCDX indexing " + warc.getId() + " " + warc.getPath());

        // fetch the list of collections from the database
        List<URL> cdxServerUrls = new ArrayList<>();
        Crawl crawl = crawls.get(warc.getCrawlId());
        Map<Long, RecordStats> collectionStats = new HashMap<>();
        for (Collection collection: collections.findByCrawlSeriesId(crawl.getCrawlSeriesId())) {
            if (collection.getCdxUrl() != null && !collection.getCdxUrl().isEmpty()) {
                cdxServerUrls.add(new URL(collection.getCdxUrl() + (deleteMode ? "/delete" : "")));
                collectionStats.put(collection.getId(), null);
            }
        }

        RecordStats stats;

        Path tempFile = Files.createTempFile("bamboo-", ".cdx");
        try (FileChannel tempChannel = FileChannel.open(tempFile, READ, WRITE, CREATE, DELETE_ON_CLOSE)) {
            PrintWriter printWriter = new PrintWriter(Channels.newOutputStream(tempChannel), false, UTF_8);
            // parse the warc file
            try (WarcReader warcReader = new WarcReader(warcs.openStream(warc))) {
                stats = Cdx.buildIndex(warcReader, printWriter, warc.getFilename(), !deleteMode);
                printWriter.flush();
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

            long cdxLength = tempChannel.size();
            // submit the records to each collection
            for (URL url : cdxServerUrls) {
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.addRequestProperty("Content-Type", "text/plain");
                if (oidc != null) {
                    connection.addRequestProperty("Authorization", oidc.accessToken().toAuthorizationHeader());
                }
                connection.setFixedLengthStreamingMode(cdxLength);
                connection.setDoOutput(true);

                try (OutputStream outputStream = connection.getOutputStream()) {
                    tempChannel.position(0);
                    Channels.newInputStream(tempChannel).transferTo(outputStream);
                }
                if (connection.getResponseCode() != 200) {
                    log.error(url + " returned " + connection.getResponseCode());
                    // try again later ?
                    return null;
                }
                StreamUtils.drain(connection.getInputStream());
            }
        }

        // update the statistics in the database
        warcs.updateRecordStats(warc.getId(), stats, deleteMode);
        collectionStats.replaceAll((i, v) -> stats);
        warcs.updateCollections(warc.getId(), collectionStats, deleteMode);

        // mark indexing as finished
        warcs.updateState(warc.getId(), deleteMode ? Warc.DELETED : Warc.CDX_INDEXED);

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
}
