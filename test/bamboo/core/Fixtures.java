package bamboo.core;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class Fixtures extends ExternalResource {

    public TemporaryFolder tmp = new TemporaryFolder();
    public long crawlSeriesId;
    public TestConfig config;
    public DbPool dbPool;
    public DAO dao;

    @Override
    protected void before() throws Throwable {
        tmp.create();
        config = new TestConfig();
        dbPool = new DbPool(config);

        dbPool.migrate();

        dao = dbPool.dao();

        dao.serieses().createCrawlSeries("Series fixture", tmp.newFolder("series-fixture").getPath());
    }

    @Override
    protected void after() {
        dbPool.close();
        tmp.delete();
    }
}
