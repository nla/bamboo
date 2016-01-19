package bamboo;

import bamboo.core.Db;
import bamboo.core.DbPool;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class Fixtures extends ExternalResource {

    public TemporaryFolder tmp = new TemporaryFolder();
    public long crawlSeriesId;
    public TestConfig config;
    public DbPool dbPool;

    @Override
    protected void before() throws Throwable {
        tmp.create();
        config = new TestConfig();
        dbPool = new DbPool(config);

        dbPool.migrate();

        try (Db db = dbPool.take()) {
            crawlSeriesId = db.createCrawlSeries("Series fixture", tmp.newFolder("series-fixture").getPath());
        }
    }

    @Override
    protected void after() {
        dbPool.close();
        tmp.delete();
    }
}
