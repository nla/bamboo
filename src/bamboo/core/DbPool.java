package bamboo.core;

import com.googlecode.flyway.core.Flyway;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.skife.jdbi.v2.DBI;

import java.io.Closeable;

public class DbPool implements Closeable {
    final HikariDataSource ds;
    final DBI dbi;

    public DbPool(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getDbUrl());
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());
        ds = new HikariDataSource(hikariConfig);
        dbi = new DBI(ds);
        dbi.registerMapper(new Db.CdxMapper());
        dbi.registerMapper(new Db.CollectionMapper());
        dbi.registerMapper(new Db.CrawlMapper());
        dbi.registerMapper(new Db.CrawlSeriesMapper());
    }

    public void migrate() {
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
