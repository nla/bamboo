package bamboo.app;

import bamboo.core.*;
import bamboo.crawl.*;
import bamboo.pandas.Pandas;
import bamboo.seedlist.Seedlists;
import bamboo.task.*;

public class Bamboo implements AutoCloseable {

    public final Config config;
    private final DbPool dbPool;
    public final Crawls crawls;
    public final Serieses serieses;
    public final Warcs warcs;
    public final Collections collections;
    public final Seedlists seedlists;
    public final Pandas pandas;

    public final Taskmaster taskmaster;

    public Bamboo() {
        this(new Config());
    }

    public Bamboo(Config config) {
        long startTime = System.currentTimeMillis();

        this.config = config;

        dbPool = new DbPool(config);
        dbPool.migrate();
        DAO dao = dbPool.dao();

        this.taskmaster = new Taskmaster();

        // crawl package
        this.serieses = new Serieses(dao.serieses());
        this.warcs = new Warcs(dao.warcs());
        this.crawls = new Crawls(dao.crawls(), serieses, warcs);
        this.collections = new Collections(dao.collections());

        // seedlist package
        this.seedlists = new Seedlists(dao.seedlists());

        // task package
        taskmaster.add(new Importer(config, crawls));
        CdxIndexer cdxIndexer = new CdxIndexer(warcs, crawls, serieses, collections);
        taskmaster.add(cdxIndexer);
        taskmaster.add(new SolrIndexer(collections, crawls, warcs));
        taskmaster.add(new WatchImporter(collections, crawls, cdxIndexer, warcs, config.getWatches()));

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
    }
}
