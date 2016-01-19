package bamboo.core;

import bamboo.util.Pager;

import java.util.List;

public class Warcs {
    private final DbPool dbPool;

    public Warcs(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public List<Warc> findByCrawlId(long crawlId) {
        try (Db db = dbPool.take()) {
            return db.findWarcsByCrawlId(crawlId);
        }
    }

    public Pager<Warc> paginateWithCrawlId(long page, long crawlId) {
        try (Db db = dbPool.take()) {
            return new Pager<>(page, db.countWarcsWithCrawlId(crawlId),
                    (limit, offset) -> db.paginateWarcsInCrawl(crawlId, limit, offset));
        }
    }

    public Pager<Warc> paginateWithCrawlIdAndState(long page, long crawlId, int state) {
        try (Db db = dbPool.take()) {
            return new Pager<>(page, db.countWarcsInCrawlAndState(crawlId, state),
                    (limit, offset) -> db.paginateWarcsInCrawlAndState(crawlId, state, limit, offset));
        }
    }

    public Pager<Warc> paginateWithState(long page, int stateId) {
        try (Db db = dbPool.take()) {
            return new Pager<>(page, db.countWarcsInState(stateId),
                    (offset, limit) -> db.paginateWarcsInState(stateId, offset, limit));
        }
    }

    public Warc getOrNull(long id) {
        try (Db db = dbPool.take()) {
            return db.findWarc(id);
        }
    }
    /**
     * Retrieve a series's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Warc get(long id) {
        return NotFoundException.check(getOrNull(id), "warc", id);
    }

    public Warc getByFilename(String filename) {
        try (Db db = dbPool.take()) {
            return NotFoundException.check(db.findWarcByFilename(filename), "warc with filename: " + filename, 0);
        }
    }

    public String stateName(int stateId) {
        try (Db db = dbPool.take()) {
            return db.findWarcStateName(stateId);
        }
    }

    @Deprecated
    public List<Warc> listAll() {
        try (Db db = dbPool.take()) {
            return db.listWarcs();
        }
    }

    public void updateSha256(long warcId, String calculatedDigest) {
        try (Db db = dbPool.take()) {
            db.updateWarcSha256(warcId, calculatedDigest);
        }
    }
}
