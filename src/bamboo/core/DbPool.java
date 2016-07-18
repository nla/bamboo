package bamboo.core;

import com.googlecode.flyway.core.Flyway;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.io.Closeable;
import java.nio.file.Path;

public class DbPool implements Closeable {
    final ViburDBCPDataSource ds;
    public final DBI dbi;
    private final DAO dao;

    public DbPool(Config config) {
        long start = System.currentTimeMillis();

        ds = new ViburDBCPDataSource();
        ds.setName("BambooDBPool");
        ds.setJdbcUrl(config.getDbUrl());
        ds.setUsername(config.getDbUser());
        ds.setPassword(config.getDbPassword());
        ds.start();

        System.out.println("Initialized connection pool in " + (System.currentTimeMillis() - start) + "ms");

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
        ds.terminate();
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
