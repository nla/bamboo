package bamboo.pandas;

import bamboo.core.Config;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Slf4JSqlLogger;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.vibur.dbcp.ViburDBCPDataSource;

public class PandasDB implements Cloneable {

    private final ViburDBCPDataSource dataSource;
    final Jdbi dbi;
    final PandasDAO dao;

    PandasDB(Config config) {
        dataSource = new ViburDBCPDataSource();
        dataSource.setName("PandasDB");
        dataSource.setJdbcUrl(config.getPandasDbUrl());
        dataSource.setUsername(config.getPandasDbUser());
        dataSource.setPassword(config.getPandasDbPassword());
        dataSource.start();

        dbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        dbi.setSqlLogger(new Slf4JSqlLogger());
        dbi.registerRowMapper(new PandasDAO.InstanceMapper(config));
        this.dao = dbi.onDemand(PandasDAO.class);
    }

    public void close() {
        dao.getHandle().close();
        dataSource.terminate();
    }
}
