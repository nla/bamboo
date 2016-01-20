package bamboo.core;

import com.googlecode.flyway.core.Flyway;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.io.Closeable;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DbPool implements Closeable {
    final HikariDataSource ds;
    public final DBI dbi;

    public DbPool(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName("BambooDbPool");
        hikariConfig.setJdbcUrl(config.getDbUrl());
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());
        ds = new HikariDataSource(hikariConfig);
        dbi = new DBI(ds);
        dbi.registerArgumentFactory(new PathArgumentFactory());

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

    private void registerMysqlCompatibilityFunctionsWithH2() {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ALIAS IF NOT EXISTS SUBSTRING_INDEX DETERMINISTIC FOR \"" + DbH2Compat.class.getName() + ".substringIndex\"");
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

    public static class PathArgumentFactory implements ArgumentFactory<Path> {
        @Override
        public boolean accepts(Class<?> aClass, Object o, StatementContext statementContext) {
            return o instanceof Path;
        }

        @Override
        public Argument build(Class<?> aClass, Path path, StatementContext statementContext) {
            return (i, stmt, ctx) -> stmt.setString(i, path.toString());
        }
    }

}
