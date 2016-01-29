package bamboo.pandas;

import bamboo.core.Config;
import bamboo.core.NotFoundException;
import bamboo.crawl.Crawl;
import bamboo.crawl.Crawls;
import bamboo.seedlist.Seedlists;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.io.IOException;

public class Pandas implements AutoCloseable {
    private final Crawls crawls;
    private final PandasDAO dao;
    private final Seedlists seedlists;
    private ViburDBCPDataSource dataSource;
    final DBI dbi;

    public Pandas(Config config, Crawls crawls, Seedlists seedlists) {
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

        this.crawls = crawls;
        this.seedlists = seedlists;
        this.dao = dbi.onDemand(PandasDAO.class);
    }

    public PandasInstance getInstance(long instanceId) {
        return NotFoundException.check(dao.findInstance(instanceId), "pandas instance", instanceId);
    }

    public void importInstance(long instanceId, long seriesId) throws IOException {
        PandasInstance instance = getInstance(instanceId);
        Crawl crawl = crawls.getByPandasInstanceIdOrNull(instanceId);
        if (crawl != null) {
            throw new RuntimeException("crawl " + crawl.getId() + " already exists for instance " + instanceId);
        }
        long crawlId = crawls.createInPlace(instance.toCrawl(), instance.warcFiles());
    }

    public PandasComparison compareSeedlist(long seedlistId) {
        return new PandasComparison(dao, seedlists, seedlistId);
    }

    @Override
    public void close() {
        dataSource.terminate();
    }

    public ResultIterator<PandasTitle> iterateTitles() {
        return dao.iterateTitles();
    }
}
