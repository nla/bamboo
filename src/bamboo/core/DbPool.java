package bamboo.core;

import com.googlecode.flyway.core.Flyway;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.logging.PrintStreamLog;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DbPool implements Closeable {
    final HikariDataSource ds;
    final DBI dbi;

    public DbPool(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName("BambooDbPool");
        hikariConfig.setJdbcUrl(config.getDbUrl());
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());
        ds = new HikariDataSource(hikariConfig);
        dbi = new DBI(ds);
        dbi.registerMapper(new Db.CollectionMapper());
        dbi.registerMapper(new Db.CollectionWithFiltersMapper());
        dbi.registerMapper(new Db.CollectionWarcMapper());
        dbi.registerMapper(new Db.CrawlMapper());
        dbi.registerMapper(new Db.CrawlWithSeriesNameMapper());
        dbi.registerMapper(new Db.CrawlSeriesMapper());
        dbi.registerMapper(new Db.CrawlSeriesWithCountMapper());
        dbi.registerMapper(new Db.SeedMapper());
        dbi.registerMapper(new Db.SeedlistMapper());
        dbi.registerMapper(new Db.WarcMapper());
        dbi.setSQLLog(new PrintStreamLog() {
            @Override
            public void logReleaseHandle(Handle h) {
                // suppress
            }

            @Override
            public void logObtainHandle(long time, Handle h) {
                // suppress
            }
        });
    }

    public void migrate() {
        if (ds.getJdbcUrl().startsWith("jdbc:h2:")) {
            DbH2Compat.register(ds);
        }
        Flyway flyway = new Flyway();
        flyway.setDataSource(ds);
        flyway.setLocations("bamboo/migrations");
        flyway.migrate();
    }

    public Db take() {
        return dbi.open(Db.class);
    }

    @Override
    public void close() {
        ds.close();
    }
}
