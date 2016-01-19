package bamboo.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.archive.url.WaybackURLKeyMaker;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.logging.PrintStreamLog;

import java.io.Closeable;

public class PandasDbPool implements Closeable {
    final HikariDataSource ds;
    final DBI dbi;

    public PandasDbPool(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName("PandasDbPool");
        hikariConfig.setJdbcUrl(config.getPandasDbUrl());
        hikariConfig.setUsername(config.getPandasDbUser());
        hikariConfig.setPassword(config.getPandasDbPassword());
        ds = new HikariDataSource(hikariConfig);
        dbi = new DBI(ds);
        dbi.registerMapper(new PandasDb.TitleMapper());
        dbi.registerMapper(new PandasDb.InstanceSummaryMapper());
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

    public PandasDb take() {
        return dbi.open(PandasDb.class);
    }

    public void close() {
        ds.close();
    }


    public static void main(String args[]) {
        WaybackURLKeyMaker keyMaker = new WaybackURLKeyMaker();
        long start = System.currentTimeMillis();
        try (PandasDbPool dbPool = new PandasDbPool(new Config());
             PandasDb db = dbPool.take()) {

            long i = 0;

            for (ResultIterator<PandasDb.Title> it = db.iterateTitles(); it.hasNext();) {
                PandasDb.Title title = it.next();
                if (title.gatherUrl != null) {
                    /*
                    try {
                        System.out.println(i + " " + keyMaker.makeKey(title.gatherUrl));
                    } catch (URISyntaxException e) {
                        e.printStackTrace();
                    }*/
                }
                i++;
            }

        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
