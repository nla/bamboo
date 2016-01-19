package bamboo;

import bamboo.core.Config;
import bamboo.core.DbPool;

import java.nio.file.Path;

public class TestConfig extends Config {
    Path heritrixJobs;

    @Override
    public String getDbUrl() {
        return "jdbc:h2:mem:bamboo-unit-test-" + Thread.currentThread().getId();
    }


    public void setHeritrixJobs(Path path) {
        this.heritrixJobs = path;
    }

    @Override
    public Path getHeritrixJobs() {
        return heritrixJobs;
    }

    public static DbPool testDbPool() {
        return new DbPool(new TestConfig());
    }
}
