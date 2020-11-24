package bamboo.app;

import bamboo.core.Config;
import bamboo.core.DAO;
import bamboo.core.DbPool;
import bamboo.core.LockManager;
import bamboo.crawl.*;
import bamboo.pandas.Pandas;
import bamboo.seedlist.Seedlists;
import bamboo.task.*;
import bamboo.util.Oidc;
import doss.BlobStore;
import doss.DOSS;
import doss.http.Credentials;
import doss.http.HttpBlobStore;
import doss.http.OAuthClientCredentials;
import doss.trivial.TrivialBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Bamboo implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Bamboo.class);

    public final Config config;
    private final DbPool dbPool;
    private final BlobStore blobStore;
    public final DAO dao;

    public final Agencies agencies;
    public final Crawls crawls;
    public final Serieses serieses;
    public final Warcs warcs;
    public final Collections collections;
    public final Seedlists seedlists;
    public final Pandas pandas;

    public final TaskManager taskManager;
    public final CdxIndexer cdxIndexer;
    private final LockManager lockManager;
    public final TextExtractor textExtractor;

    public Bamboo(Config config, boolean runTasks) throws IOException {
        long startTime = System.currentTimeMillis();

        this.config = config;

        Oidc oidc = null;
        if (config.getOidcUrl() != null) {
            oidc = new Oidc(config.getOidcUrl(), config.getOidcClientId(), config.getOidcClientSecret());
        }

        String dossUrl = config.getDossUrl();
        if (dossUrl == null) {
            Path blobsDir = Paths.get("data/blobs");
            Files.createDirectories(blobsDir);
            blobStore = new TrivialBlobStore(blobsDir);
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

        textExtractor = new TextExtractor();
        this.taskManager = new TaskManager(dao.tasks());
        this.lockManager = new LockManager(dao.lockManager());

        // crawl package
        this.agencies = new Agencies(dao.agency());
        this.serieses = new Serieses(dao.serieses());
        this.warcs = new Warcs(dao.warcs(), blobStore, config.getWarcUrl());
        this.crawls = new Crawls(dao.crawls(), serieses, warcs, blobStore);
        this.collections = new Collections(dao.collections());

        // seedlist package
        this.seedlists = new Seedlists(dao.seedlists());

        // task package
        taskManager.register(new Importer(config, crawls, lockManager));
        if (config.getCdxIndexerThreads() > 0) {
            cdxIndexer = new CdxIndexer(warcs, crawls, collections, lockManager, oidc, config.getCdxIndexerThreads());
            taskManager.register(cdxIndexer);
            taskManager.register(new WatchImporter(collections, crawls, cdxIndexer, warcs, config.getWatches()));
        } else {
            log.warn("CDX indexing disabled (CDX_INDEXER_THREADS=0)");
            cdxIndexer = null;
        }
        if (runTasks && config.isTasksEnabled()) {
            taskManager.start();
        }

        // pandas package
        if (config.getPandasDbUrl() != null) {
            pandas = new Pandas(config, crawls, seedlists, dao.agency());
            pandas.syncAgencies();
        } else {
            pandas = null;
        }

        log.info("Initialized Bamboo in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    public void close() {
        taskManager.close();
        dbPool.close();
        if (pandas != null) {
            pandas.close();
        }
        lockManager.close();
        textExtractor.close();
    }

    public boolean healthcheck(PrintWriter out) {
        boolean allOk = dbPool.healthcheck(out) &
                warcs.healthcheck(out) &
                (cdxIndexer == null || cdxIndexer.healthcheck(out));
        if (allOk) {
            out.println("\nALL OK");
        }
        return allOk;
    }

    public DataSource getDataSource() {
        return dbPool.getDataSource();
    }
}
