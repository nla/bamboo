package bamboo.crawl;

import bamboo.core.DbPool;
import bamboo.core.NotFoundException;
import bamboo.util.Pager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Crawls {
    private final CrawlsDAO dao;

    public Crawls(DbPool dbPool) {
        this.dao = dbPool.dbi.onDemand(CrawlsDAO.class);
    }

    /**
     * Create a crawl based on a set of existing WARC files without moving them.
     *
     * @return the id of the new crawl
     */
    public long createInPlace(Crawl metadata, Collection<Path> warcPaths) throws IOException {
        List<Warc> warcs = new ArrayList<>();
        for (Path path: warcPaths) {
            warcs.add(Warc.fromFile(path));
        }

        return dao.inTransaction((dao1, ts) -> {
            long totalBytes = warcs.stream().mapToLong(Warc::getSize).sum();
            long crawlId = dao.createCrawl(metadata);
            dao.warcs().batchInsertWarcsWithoutRollup(crawlId, warcs.iterator());
            dao.warcs().incrementWarcStatsForCrawl(crawlId, warcs.size(), totalBytes);
            return crawlId;
        });
    }

    public Crawl getOrNull(long crawlId) {
            return dao.findCrawl(crawlId);
    }

    /**
     * Retrieve a crawl's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Crawl get(long crawlId) {
        return NotFoundException.check(getOrNull(crawlId), "crawl", crawlId);
    }

    /**
     * Retrieve various statistics about this crawl (number of WARC files etc).
     */
    public CrawlStats stats(long crawlId) {
            return new CrawlStats(dao, crawlId);
    }

    /**
     * Update a crawl
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public void update(long crawlId, String name, String description) {
        if (description != null && description.isEmpty()) {
            description = null;
        }

            int rows = dao.updateCrawl(crawlId, name, description);
            if (rows == 0) {
                throw new NotFoundException("crawl", crawlId);
            }
    }

    public Pager<CrawlAndSeriesName> pager(long page) {
            return new Pager<>(page, dao.countCrawls(), dao::paginateCrawlsWithSeriesName);
    }

    public List<Crawl> listWhereSeriesId(long seriesId) {
            return dao.findCrawlsByCrawlSeriesId(seriesId);
    }

}
