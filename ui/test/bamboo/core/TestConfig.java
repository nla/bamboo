package bamboo.core;

import java.nio.file.Path;

public class TestConfig extends Config {
    Path heritrixJobs;
    private Path pandasWarcDir;

    @Override
    public String getDbUrl() {
        return "jdbc:h2:mem:bamboo-unit-test-" + Thread.currentThread().getId() + ";mode=MySQL";
    }

    @Override
    public String getPandasDbUrl() {
        return "jdbc:h2:mem:pandas-unit-test-" + Thread.currentThread().getId();
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

    public void setPandasWarcDir(Path pandasWarcDir) {
        this.pandasWarcDir = pandasWarcDir;
    }

    @Override
    public Path getPandasWarcDir() {
        return pandasWarcDir;
    }
}
