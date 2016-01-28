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

public class DbPool implements Closeable {
    final HikariDataSource ds;
    public final DBI dbi;
    private final DAO dao;

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

        dao = dbi.onDemand(DAO.class);
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

    public DAO dao() {
        return dao;
    }

    public DAO take() {
        return dbi.open(DAO.class);
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
