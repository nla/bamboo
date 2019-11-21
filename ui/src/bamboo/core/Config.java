package bamboo.core;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Config {
    private final Map<String, String> env;

    public Config(Map<String, String> env) {
        this.env = env;
    }

    private String getEnv(String name, String defaultValue) {
        return env.getOrDefault(name, defaultValue);
    }

    public int getCdxIndexerThreads() {
        String value = getEnv("CDX_INDEXER_THREADS", null);
        if (value == null) {
            return Runtime.getRuntime().availableProcessors();
        }
        return Integer.parseInt(value);
    }

    public String getDbUser() {
        return getEnv("BAMBOO_DB_USER", "bamboo");
    }

    public String getDbPassword() {
        return getEnv("BAMBOO_DB_PASSWORD", "bamboo");
    }

    public String getDbUrl() {
        return getEnv("BAMBOO_DB_URL", "jdbc:h2:mem:bamboo;mode=mysql;db_close_delay=-1");
    }

    public Path getHeritrixJobs() {
        return Paths.get(getEnv("HERITRIX_JOBS", "/heritrix/jobs"));
    }

    public String getHeritrixUrl() {
        return getEnv("HERITRIX_URL", "https://127.0.0.1:8443");
    }

    public String getDossUrl() {
        return getEnv("DOSS_URL", null);
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
        String value = getEnv("PANDAS_WARC_DIR", null);
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

    public static class Watch {
        public final long crawlId;
        public final Path dir;

        public Watch(Path dir, long crawlId) {
            this.crawlId = crawlId;
            this.dir = dir;
        }
    }
}
