package bamboo.app;

import bamboo.core.*;
import bamboo.crawl.Collections;
import bamboo.crawl.Crawls;
import bamboo.crawl.Serieses;
import bamboo.crawl.Warcs;
import bamboo.pandas.Pandas;
import bamboo.seedlist.Seedlists;
import bamboo.task.CdxIndexer;
import bamboo.task.Importer;
import bamboo.task.SolrIndexer;
import bamboo.task.WatchImporter;
import bamboo.util.Oidc;
import doss.BlobStore;
import doss.DOSS;

import java.io.IOException;
import java.io.PrintWriter;

public class Bamboo implements AutoCloseable {

    public final Config config;
    private final DbPool dbPool;
    private final BlobStore blobStore;

    public final Crawls crawls;
    public final Serieses serieses;
    public final Warcs warcs;
    public final Collections collections;
    public final Seedlists seedlists;
    public final Pandas pandas;

    public final Taskmaster taskmaster;
    public final CdxIndexer cdxIndexer;
    private SolrIndexer solrIndexer;
    private final LockManager lockManager;

    public Bamboo(boolean runTasks) throws IOException {
        this(new Config(), runTasks);
    }

    public Bamboo(Config config, boolean runTasks) throws IOException {
        long startTime = System.currentTimeMillis();

        this.config = config;

        Oidc oidc = null;
        if (config.getOidcUrl() != null) {
            oidc = new Oidc(config.getOidcUrl(), config.getOidcClientId(), config.getOidcClientSecret());
        }

        String dossUrl = config.getDossUrl();
        blobStore = dossUrl == null ? null : DOSS.open(dossUrl);

        dbPool = new DbPool(config);
        dbPool.migrate();
        DAO dao = dbPool.dao();

        this.taskmaster = new Taskmaster();
        this.lockManager = new LockManager(dao.lockManager());

        // crawl package
        this.serieses = new Serieses(dao.serieses());
        this.warcs = new Warcs(dao.warcs(), blobStore, config.getWarcUrl());
        this.crawls = new Crawls(dao.crawls(), serieses, warcs);
        this.collections = new Collections(dao.collections());

        // seedlist package
        this.seedlists = new Seedlists(dao.seedlists());

        // task package
        taskmaster.add(new Importer(config, crawls));
        cdxIndexer = new CdxIndexer(warcs, crawls, collections, lockManager, oidc);
        taskmaster.add(cdxIndexer);
        if (!config.getNoSolr()) {
            solrIndexer = new SolrIndexer(collections, crawls, warcs, lockManager);
            taskmaster.add(solrIndexer);
        }
        taskmaster.add(new WatchImporter(collections, crawls, cdxIndexer, warcs, config.getWatches()));
        if (runTasks) {
            taskmaster.startAll();
        }

        // pandas package
        if (config.getPandasDbUrl() != null) {
            pandas = new Pandas(config, crawls, seedlists);
        } else {
            pandas = null;
        }

        System.out.println("Initialized Bamboo in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    public void close() {
        taskmaster.close();
        dbPool.close();
        pandas.close();
        lockManager.close();
    }

    public boolean healthcheck(PrintWriter out) {
        boolean allOk = dbPool.healthcheck(out) &
                warcs.healthcheck(out) &
                cdxIndexer.healthcheck(out) &
                (solrIndexer == null || solrIndexer.healthcheck(out));
        if (allOk) {
            out.println("\nALL OK");
        }
        return allOk;
    }
}
