package bamboo.task;

import bamboo.core.LockManager;
import bamboo.crawl.*;
import bamboo.util.SurtFilter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.url.SURT;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SolrIndexer implements Runnable {

    static final int BATCH_SIZE = 1024;
    static final int COMMIT_WITHIN_MS = 300000;

    private final Crawls crawls;
    private final Warcs warcs;
    private final Collections collections;
    private SolrServer solr;

    private static TextExtractor extractor = new TextExtractor();
    private final LockManager lockManager;

    public SolrIndexer(Collections collections, Crawls crawls, Warcs warcs, LockManager lockManager) {
        this.crawls = crawls;
        this.collections = collections;
        this.warcs = warcs;
        this.lockManager = lockManager;
    }

    public void run() {
        while (true) {
            List<Warc> warcList = warcs.findByState(Warc.CDX_INDEXED, BATCH_SIZE);

            if (warcList.isEmpty()) {
                break;
            }

            indexWarcs(warcList);
        }
    }

    public void indexWarcs(List<Warc> warcs) {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        try {
            for (Warc warc : warcs) {
                threadPool.submit(() -> {
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
                });
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdownNow();
        }
    }

    public boolean healthcheck(PrintWriter out) {
        boolean ok = true;
        System.out.print("Checking Solr indexes are reachable... ");
        for (Collection collection : collections.listAll()) {
            String url = collection.getSolrUrl();
            if (url != null && !url.isEmpty()) {
                url += "/select";
                String ref = "[collection/" + collection.getId() + " " + url + "]";
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
        return ok;
    }

    static class Solr {
        private final SurtFilter filter;
        private final SolrServer server;
        private final Collection collection;

        public Solr(CollectionWithFilters collection) {
            this.collection = collection;
            server = new HttpSolrServer(collection.getSolrUrl());
            filter = new SurtFilter(collection.urlFilters);
        }

        public void add(String surt, SolrInputDocument doc) throws IOException, SolrServerException {
            if (filter.accepts(surt)) {
                server.add(doc, COMMIT_WITHIN_MS);
            }
        }
    }

    boolean anyFilterAccepts(List<Solr> solrs, String surt) {
        for (Solr solr : solrs) {
            if (solr.filter.accepts(surt)) {
                return true;
            }
        }
        return false;
    }

    void indexWarc(Warc warc) {
        System.out.println(new Date() +  " Solr indexing " + warc.getId() + " " + warc.getPath());

        List<Solr> solrs = new ArrayList<>();

        Crawl crawl = crawls.get(warc.getCrawlId());
        List<CollectionWithFilters> collectionList = collections.listWhereSeriesId(crawl.getCrawlSeriesId());
        for (CollectionWithFilters collection : collectionList) {
            if (collection.getSolrUrl() != null && !collection.getSolrUrl().isEmpty()) {
                solrs.add(new Solr(collection));
            }
        }

        List<SolrInputDocument> batch = new ArrayList<>();
        try (ArchiveReader reader = WarcUtils.open(warc.getPath())) {
            for (ArchiveRecord record : reader) {
                if (record.getHeader().getUrl() == null) continue;
                String surt = SURT.toSURT(WarcUtils.getCleanUrl(record.getHeader()));
                if (surt == null || !anyFilterAccepts(solrs, surt)) {
                    continue; // skip indexing records we're not going to accept anyway
                }

                SolrInputDocument doc = makeDoc(record);
                if (doc == null) continue;

                for (Solr solr : solrs) {
                    try {
                        solr.add(surt, doc);
                    } catch (RuntimeException e) {
                        System.err.println("Error indexing " + doc.get("id"));
                        throw e;
                    }
                }
            }

            warcs.updateState(warc.getId(), Warc.SOLR_INDEXED);
            System.out.println(new Date() + " Finished Solr indexing " + warc.getId() + " " + warc.getPath());
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static SolrInputDocument makeDoc(ArchiveRecord record) throws IOException {
        Document doc;
        try {
            doc = extractor.extract(record);
        } catch (TextExtractionException e) {
            return null;
        }

        if (doc.getStatusCode() < 200 || doc.getStatusCode() > 299) {
            return null;
        }

        if (doc.getText() == null || doc.getText().isEmpty()) {
            return null;
        }

        SolrInputDocument solrDoc = new SolrInputDocument();
        solrDoc.addField("id", doc.getSite() + "!" + doc.getUrl() + " " + WarcUtils.arcDateFormat.format(doc.getDate().toInstant()));
        solrDoc.addField("url", doc.getUrl());
        solrDoc.addField("length", doc.getContentLength());
        solrDoc.addField("code", doc.getStatusCode());
        solrDoc.addField("date", doc.getDate());
        solrDoc.addField("site", doc.getSite());
        solrDoc.addField("type", doc.getContentType());

        if (doc.getSha1() != null) {
            solrDoc.addField("sha1", doc.getSha1());
        }

        return solrDoc;
    }

    public static void main(String args[]) {
        for (String arg : args) {
            try (ArchiveReader reader = ArchiveReaderFactory.get(arg)) {
                for (ArchiveRecord record : reader) {
                    SolrInputDocument doc = makeDoc(record);
                    if (doc != null) {
                        System.out.println(doc.toString());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
