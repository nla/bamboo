package bamboo.core;

import bamboo.util.H2Functions;
import com.googlecode.flyway.core.Flyway;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.skife.jdbi.v2.DBI;
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
        hikariConfig.setJdbcUrl(config.getDbUrl());
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());
        ds = new HikariDataSource(hikariConfig);
        dbi = new DBI(ds);
        dbi.registerMapper(new Db.CollectionMapper());
        dbi.registerMapper(new Db.CrawlMapper());
        dbi.registerMapper(new Db.CrawlSeriesMapper());
        dbi.registerMapper(new Db.WarcMapper());
        dbi.registerMapper(new Db.CollectionWithFiltersMapper());
        dbi.setSQLLog(new PrintStreamLog());
    }

    public void migrate() {
        if (ds.getJdbcUrl().startsWith("jdbc:h2:")) {
            registerMysqlCompatibilityFunctionsWithH2();
        }
        Flyway flyway = new Flyway();
        flyway.setDataSource(ds);
        flyway.setLocations("bamboo/migrations");
        flyway.migrate();
    }

    private void registerMysqlCompatibilityFunctionsWithH2() {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ALIAS IF NOT EXISTS SUBSTRING_INDEX DETERMINISTIC FOR \"" + H2Functions.class.getName() + ".substringIndex\"");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Db take() {
        return dbi.open(Db.class);
    }

    @Override
    public void close() {
        ds.close();
    }
}
