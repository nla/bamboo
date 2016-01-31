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
import java.util.List;

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

    public void importAllInstances(long seriesId) throws IOException {
        int batchSize = 100;
        long prev = -1;
        List<Long> instanceIds;

        do {
            instanceIds = dao.listArchivedInstanceIds(prev, batchSize);
            for (long id : instanceIds) {
                Long crawlId = importInstanceIfNotExists(id, seriesId);
                if (crawlId != null) {
                    System.out.println("Instance " + instanceIds + " imported as crawl " + crawlId);
                } else {
                    System.out.println("Instance " + instanceIds + " already imported.");
                }
                prev = id;
            }
        } while (!instanceIds.isEmpty());
    }

    public Long importInstanceIfNotExists(long instanceId, long seriesId) throws IOException {
        Crawl existing = crawls.getByPandasInstanceIdOrNull(instanceId);
        if (existing != null) {
            return null;
        }
        PandasInstance instance = getInstance(instanceId);
        Crawl crawl = instance.toCrawl();
        crawl.setCrawlSeriesId(seriesId);
        long crawlId = crawls.createInPlace(crawl, instance.warcFiles());
        return crawlId;
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
