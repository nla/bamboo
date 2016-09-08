package bamboo.trove.db;

import java.io.Closeable;
import java.nio.file.Path;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPDataSource;

public class DbPool implements Closeable {
  private static Logger log = LoggerFactory.getLogger(DbPool.class);
  private final ViburDBCPDataSource ds;
  private final DBI dbi;
  private final TroveDaoRegistry dao;

  public DbPool(String jdbcUrl, String username, String password) {
    ds = new ViburDBCPDataSource();
    ds.setName("TroveDBPool");
    ds.setJdbcUrl(jdbcUrl);
    ds.setUsername(username);
    ds.setPassword(password);
    ds.start();
    log.debug("Connected to Trove Database");

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
    dao = dbi.onDemand(TroveDaoRegistry.class);
  }

  public TroveDaoRegistry dao() {
    return dao;
  }

  public TroveDaoRegistry take() {
    return dbi.open(TroveDaoRegistry.class);
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