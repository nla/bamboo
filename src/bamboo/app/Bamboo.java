package bamboo.app;

import bamboo.core.*;
import bamboo.crawl.*;
import bamboo.seedlist.Seedlists;
import bamboo.task.*;

public class Bamboo implements AutoCloseable {

    public final Config config;
    private final DbPool dbPool;
    public final PandasDbPool pandasDbPool;

    public final Crawls crawls;
    public final Serieses serieses;
    public final Warcs warcs;
    public final Collections collections;
    public final Seedlists seedlists;

    public final Taskmaster taskmaster;

    public Bamboo() {
        this(new Config());
    }

    public Bamboo(Config config) {
        this.config = config;

        dbPool = new DbPool(config);
        dbPool.migrate();
        DAO dao = dbPool.dao();

        pandasDbPool = new PandasDbPool(config);

        this.taskmaster = new Taskmaster();

        // crawl package
        this.serieses = new Serieses(dao.serieses());
        this.crawls = new Crawls(dao.crawls(), serieses);
        this.warcs = new Warcs(dao.warcs());
        this.collections = new Collections(dao.collections());

        // seedlist package
        this.seedlists = new Seedlists(dao.seedlists());

        // task package
        taskmaster.add(new Importer(config, crawls));
        CdxIndexer cdxIndexer = new CdxIndexer(warcs, crawls, serieses, collections);
        taskmaster.add(cdxIndexer);
        taskmaster.add(new SolrIndexer(collections, crawls, warcs));
        taskmaster.add(new WatchImporter(collections, crawls, cdxIndexer, warcs, config.getWatches()));
    }

    public void close() {
        taskmaster.close();
        dbPool.close();
    }
}
