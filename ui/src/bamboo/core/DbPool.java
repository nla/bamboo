package bamboo.core;

import org.hibernate.type.SqlTypes;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.argument.ArgumentFactory;
import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.statement.Slf4JSqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.transaction.SerializableTransactionRunner;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.vibur.dbcp.ViburDBCPDataSource;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.Optional;

public class DbPool implements Closeable {
    final ViburDBCPDataSource ds;
    public final Jdbi dbi;
    private final DAO dao;

    public DbPool(Config config) {
        long start = System.currentTimeMillis();

        ds = new ViburDBCPDataSource();
        ds.setPoolInitialSize(1);
        ds.setPoolMaxSize(32);
        ds.setName("BambooDBPool");
        ds.setJdbcUrl(config.getDbUrl());
        ds.setUsername(config.getDbUser());
        ds.setPassword(config.getDbPassword());
        ds.start();

        System.out.println("Initialized connection pool in " + (System.currentTimeMillis() - start) + "ms");

        dbi = Jdbi.create(ds).installPlugin(new SqlObjectPlugin());
        dbi.setTransactionHandler(new SerializableTransactionRunner());
        dbi.registerArgument(new PathArgumentFactory());

        if (System.getenv("SQL_LOG") != null) {
            dbi.setSqlLogger(new Slf4JSqlLogger());
        }

        dao = dbi.onDemand(DAO.class);
    }

    public void migrate() {
        if (ds.getJdbcUrl().startsWith("jdbc:h2:")) {
            DbH2Compat.register(ds);
        }
// Flyway 5+        Flyway.configure().dataSource(ds).locations("bamboo/migrations").load().migrate();
        Flyway flyway = new Flyway();
        flyway.setDataSource(ds);
        flyway.setLocations("bamboo/migrations");
        flyway.migrate();
    }


    public DAO dao() {
        return dao;
    }

    @Override
    public void close() {
        ds.terminate();
    }

    public boolean healthcheck(PrintWriter out) {
        out.print("Checking database connection... ");
        try (Handle h = dbi.open()) {
            boolean ok = !h.select("select 1").mapToMap().findOne().isEmpty();
            out.println(ok ? "OK" : "FAILED");
            return ok;
        } catch (Exception e) {
            out.println("ERROR");
            e.printStackTrace(out);
            return false;
        }
    }

    public DataSource getDataSource() {
        return ds;
    }

    public static class PathArgumentFactory extends AbstractArgumentFactory<Path> {
        protected PathArgumentFactory() {
            super(SqlTypes.VARCHAR);
        }

        @Override
        protected Argument build(Path path, ConfigRegistry config) {
            return (i, stmt, ctx) -> stmt.setString(i, path.toString());
        }
    }

}
