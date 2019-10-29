package bamboo.app;

import bamboo.core.Config;
import bamboo.core.DAO;
import bamboo.core.DbPool;
import bamboo.core.LockManager;
import bamboo.crawl.Collections;
import bamboo.crawl.Crawls;
import bamboo.crawl.Serieses;
import bamboo.crawl.Warcs;
import bamboo.pandas.Pandas;
import bamboo.seedlist.Seedlists;
import bamboo.task.CdxIndexer;
import bamboo.task.Importer;
import bamboo.task.TaskManager;
import bamboo.task.WatchImporter;
import bamboo.util.Oidc;
import doss.BlobStore;
import doss.DOSS;
import doss.http.Credentials;
import doss.http.HttpBlobStore;
import doss.http.OAuthClientCredentials;
import doss.trivial.TrivialBlobStore;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

public class Bamboo implements AutoCloseable {

    public final Config config;
    private final DbPool dbPool;
    private final BlobStore blobStore;
    public final DAO dao;

    public final Crawls crawls;
    public final Serieses serieses;
    public final Warcs warcs;
    public final Collections collections;
    public final Seedlists seedlists;
    public final Pandas pandas;

    public final TaskManager taskManager;
    public final CdxIndexer cdxIndexer;
    private final LockManager lockManager;

    public Bamboo(Config config, boolean runTasks) throws IOException {
        long startTime = System.currentTimeMillis();

        this.config = config;

        Oidc oidc = null;
        if (config.getOidcUrl() != null) {
            oidc = new Oidc(config.getOidcUrl(), config.getOidcClientId(), config.getOidcClientSecret());
        }

        String dossUrl = config.getDossUrl();
        if (dossUrl == null) {
            blobStore = new TrivialBlobStore(Paths.get("data/blobs"));
        } else if (dossUrl.startsWith("http:") || dossUrl.startsWith("https:")) {
            Credentials dossCredentials;
            if (config.getOidcUrl() != null) {
                dossCredentials = new OAuthClientCredentials(config.getOidcUrl(), config.getOidcClientId(), config.getOidcClientSecret());
            } else {
                dossCredentials = null;
            }
            blobStore = new HttpBlobStore(dossUrl, dossCredentials);
        } else {
            blobStore = DOSS.open(dossUrl);
        }

        dbPool = new DbPool(config);
        dbPool.migrate();
        dao = dbPool.dao();

        this.taskManager = new TaskManager(dao.tasks());
        this.lockManager = new LockManager(dao.lockManager());

        // crawl package
        this.serieses = new Serieses(dao.serieses());
        this.warcs = new Warcs(dao.warcs(), blobStore, config.getWarcUrl());
        this.crawls = new Crawls(dao.crawls(), serieses, warcs, blobStore);
        this.collections = new Collections(dao.collections());

        // seedlist package
        this.seedlists = new Seedlists(dao.seedlists());

        // task package
        taskManager.register(new Importer(config, crawls, lockManager));
        cdxIndexer = new CdxIndexer(warcs, crawls, collections, lockManager, oidc);
        taskManager.register(cdxIndexer);
        taskManager.register(new WatchImporter(collections, crawls, cdxIndexer, warcs, config.getWatches()));
        if (runTasks) {
            taskManager.start();
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
        taskManager.close();
        dbPool.close();
        pandas.close();
        lockManager.close();
    }

    public boolean healthcheck(PrintWriter out) {
        boolean allOk = dbPool.healthcheck(out) &
                warcs.healthcheck(out) &
                cdxIndexer.healthcheck(out);
        if (allOk) {
            out.println("\nALL OK");
        }
        return allOk;
    }
}
