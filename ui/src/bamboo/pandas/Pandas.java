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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Pandas implements AutoCloseable {
    private final Crawls crawls;
    private final PandasDAO dao;
    private final Seedlists seedlists;
    final PandasDB db;

    public Pandas(Config config, Crawls crawls, Seedlists seedlists) {
        this.db = new PandasDB(config);
        this.dao = db.dao;
        this.crawls = crawls;
        this.seedlists = seedlists;
    }

    public PandasInstance getInstance(long instanceId) {
        return NotFoundException.check(dao.findInstance(instanceId), "pandas instance", instanceId);
    }


    public void importAllInstances(long seriesId) throws IOException {
        importAllInstances(seriesId, null);
    }

    public void importAllInstances(long seriesId, String type) throws IOException {
        int batchSize = 100;
        long prev = -1;
        List<Long> instanceIds;

        do {
            if (type == null) {
                instanceIds = dao.listArchivedInstanceIds(prev, batchSize);
            } else {
                instanceIds = dao.listArchivedInstanceIds(type, prev, batchSize);
            }

            for (long id : instanceIds) {
                Long crawlId = importInstanceIfNotExists(id, seriesId);
                if (crawlId != null) {
                    System.out.println("Instance " + id + " imported as crawl " + crawlId);
                } else {
                    System.out.println("Instance " + id + " already imported.");
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

    public void importInstanceArtifacts(long crawlId) throws IOException {
        Crawl crawl = crawls.get(crawlId);
        PandasInstance instance = dao.findInstance(crawl.getPandasInstanceId());

        String masterDir = "/sam/master/data/nla.arc";

        String dateOnly = instance.legacyDate();
        String dateTime = instance.date;

        tryImportArtifact(crawlId, "PANDAS1_ACCESS",   String.format("%s/access/arc1/%03d/%d/ac-ar1-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateOnly));
        tryImportArtifact(crawlId, "PANDAS1_PRESERVE", String.format("%s/access/arc1/%03d/%d/ps-ar1-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateOnly));
        tryImportArtifact(crawlId, "PANDAS1_MIME",     String.format("%s/access/arc1/%03d/%d/mi-ar1-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateOnly));

        tryImportArtifact(crawlId, "PANDAS2_ACCESS",   String.format("%s/access/arc2/%03d/%d/ac-ar2-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateOnly));
        tryImportArtifact(crawlId, "PANDAS2_PRESERVE", String.format("%s/access/arc2/%03d/%d/ps-ar2-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateOnly));
        tryImportArtifact(crawlId, "PANDAS2_MIME",     String.format("%s/access/arc2/%03d/%d/mi-ar2-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateOnly));

        tryImportArtifact(crawlId, "PANDAS3_ACCESS",   String.format("%s/access/arc3/%03d/%d/ac-ar2-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateTime));
        tryImportArtifact(crawlId, "PANDAS3_PRESERVE", String.format("%s/access/arc3/%03d/%d/ps-ar2-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateTime));
        tryImportArtifact(crawlId, "PANDAS3_MIME",     String.format("%s/access/arc3/%03d/%d/mi-ar2-%d-%s.tgz", masterDir, instance.pi / 1000, instance.pi, instance.pi, dateTime));
    }

    private void tryImportArtifact(long crawlId, String type, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        if (!Files.exists(path)) {
            System.out.println("Doesn't exist: " + pathStr);
            return;
        }
        String relpath = path.getFileName().toString();
        if (crawls.getArtifactByRelpathOrNull(crawlId, relpath) != null) {
            return; // already imported
        }
        System.out.println("Crawl " + crawlId + " added artifact " + path);
        crawls.addArtifact(crawlId, type, path, relpath);
    }

    public PandasComparison compareSeedlist(long seedlistId) {
        return new PandasComparison(dao, seedlists, seedlistId);
    }

    @Override
    public void close() {
        db.close();
    }

    public ResultIterator<PandasTitle> iterateTitles() {
        return dao.iterateTitles();
    }
}
