package bamboo.core;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Config {
    private String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value != null) {
            return value;
        } else {
            return defaultValue;
        }
    }

    public String getDbUser() {
        return getEnv("BAMBOO_DB_USER", "bamboo");
    }

    public String getDbPassword() {
        return getEnv("BAMBOO_DB_PASSWORD", "bamboo");
    }

    public String getDbUrl() {
        return getEnv("BAMBOO_DB_URL", "jdbc:h2:mem:bamboo");
    }

    public Path getHeritrixJobs() {
        return Paths.get(getEnv("HERITRIX_JOBS", "/heritrix/jobs"));
    }

    public String getHeritrixUrl() {
        return getEnv("HERITRIX_URL", "https://127.0.0.1:8443");
    }

    public String getDossUrl() {
        return System.getenv("DOSS_URL");
    }

    public String getPandasDbUrl() {
        return getEnv("PANDAS_DB_URL", null);
    }

    public String getPandasDbUser() {
        return getEnv("PANDAS_DB_USER", null);
    }

    public String getPandasDbPassword() {
        return getEnv("PANDAS_DB_PASSWORD", null);
    }

    public List<Watch> getWatches() {
        List<Watch> watches = new ArrayList<>();
        String value = getEnv("BAMBOO_WATCH", "");
        if (!value.isEmpty()) {
            for (String entry : value.split(",")) {
                String[] fields = entry.split(":");
                watches.add(new Watch(Paths.get(fields[1]), Long.parseLong(fields[0])));
            }
        }
        return Collections.unmodifiableList(watches);
    }

    public Path getPandasWarcDir() {
        String value = System.getenv("PANDAS_WARC_DIR");
        if (value == null) {
            throw new RuntimeException("PANDAS_WARC_DIR environment variable is not set");
        }
        return Paths.get(value);
    }

    public String getWarcUrl() {
        return getEnv("WARC_URL", null);
    }

    public String getOidcUrl() {
        return getEnv("OIDC_URL", null);
    }

    public String getOidcClientId() {
        return getEnv("OIDC_CLIENT_ID", null);
    }

    public String getOidcClientSecret() {
        return getEnv("OIDC_CLIENT_SECRET", null);
    }

    public boolean getNoSolr() {
        return Integer.parseInt(getEnv("NO_SOLR", "0")) > 0;
    }

    public static class Watch {
        public final long crawlId;
        public final Path dir;

        public Watch(Path dir, long crawlId) {
            this.crawlId = crawlId;
            this.dir = dir;
        }
    }
}
