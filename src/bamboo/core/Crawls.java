package bamboo.core;

import bamboo.util.Pager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Crawls {
    final DbPool dbPool;

    public Crawls(DbPool dbPool) {
        this.dbPool = dbPool;
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

        try (Db db = dbPool.take()) {
            return db.createCrawlWithWarcs(metadata, warcs);
        }
    }

    public Crawl getOrNull(long crawlId) {
        try (Db db = dbPool.take()) {
            return db.findCrawl(crawlId);
        }
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
        try (Db db = dbPool.take()) {
            return new CrawlStats(db, crawlId);
        }
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

        try (Db db = dbPool.take()) {
            int rows = db.updateCrawl(crawlId, name, description);
            if (rows == 0) {
                throw new NotFoundException("crawl", crawlId);
            }
        }
    }

    public Pager<CrawlAndSeriesName> pager(long page) {
        try (Db db = dbPool.take()) {
            return new Pager<>(page, db.countCrawls(), db::paginateCrawlsWithSeriesName);
        }
    }

    public List<Crawl> listWhereSeriesId(long seriesId) {
        try (Db db = dbPool.take()) {
            return db.findCrawlsByCrawlSeriesId(seriesId);
        }

    }
}
