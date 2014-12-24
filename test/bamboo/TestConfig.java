package bamboo;

import java.nio.file.Path;

public class TestConfig extends Config {
    Path heritrixJobs;

    @Override
    public String getDbUrl() {
        return "jdbc:h2:mem:bamboo-unit-test";
    }


    public void setHeritrixJobs(Path path) {
        this.heritrixJobs = path;
    }

    @Override
    public Path getHeritrixJobs() {
        return heritrixJobs;
    }
}
