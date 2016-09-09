package bamboo.pandas;

import bamboo.core.Config;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.vibur.dbcp.ViburDBCPDataSource;

public class PandasDB implements Cloneable {

    private final ViburDBCPDataSource dataSource;
    final DBI dbi;
    final PandasDAO dao;

    PandasDB(Config config) {
        dataSource = new ViburDBCPDataSource();
        dataSource.setName("PandasDB");
        dataSource.setJdbcUrl(config.getPandasDbUrl());
        dataSource.setUsername(config.getPandasDbUser());
        dataSource.setPassword(config.getPandasDbPassword());
        dataSource.start();

        dbi = new DBI(dataSource);
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
        dbi.registerMapper(new PandasDAO.InstanceMapper(config));
        this.dao = dbi.onDemand(PandasDAO.class);
    }

    public void close() {
        dao.close();
        dataSource.terminate();
    }
}
