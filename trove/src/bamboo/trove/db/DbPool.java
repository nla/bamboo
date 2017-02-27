/*
 * Copyright 2016-2017 National Library of Australia
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bamboo.trove.db;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.io.Closeable;
import java.nio.file.Path;

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
    // Turn this on if you want to see SQL statements.
    // There will be thousands per second in production contexts,
    // so we aren't even bothering to make it a config option
    boolean iWantToLogEverySqlStatement = false;
    if (iWantToLogEverySqlStatement) {
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
    dao = dbi.onDemand(TroveDaoRegistry.class);
  }

  public TroveDaoRegistry dao() {
    return dao;
  }

  @SuppressWarnings("unused")
  public TroveDaoRegistry take() {
    return dbi.open(TroveDaoRegistry.class);
  }

  @Override
  public void close() {
    ds.terminate();
  }

  private static class PathArgumentFactory implements ArgumentFactory<Path> {
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
