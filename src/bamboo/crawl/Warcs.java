package bamboo.crawl;

import bamboo.core.DbPool;
import bamboo.core.NotFoundException;
import bamboo.util.Pager;

import java.util.List;

public class Warcs {
    private final WarcsDAO dao;

    public Warcs(DbPool dbPool) {
        this.dao = dbPool.dbi.onDemand(WarcsDAO.class);
    }

    public List<Warc> findByCrawlId(long crawlId) {
        return dao.findWarcsByCrawlId(crawlId);
    }

    public Pager<Warc> paginateWithCrawlId(long page, long crawlId) {
        return new Pager<>(page, dao.countWarcsWithCrawlId(crawlId),
                (limit, offset) -> dao.paginateWarcsInCrawl(crawlId, limit, offset));
    }

    public Pager<Warc> paginateWithCrawlIdAndState(long page, long crawlId, int state) {
        return new Pager<>(page, dao.countWarcsInCrawlAndState(crawlId, state),
                (limit, offset) -> dao.paginateWarcsInCrawlAndState(crawlId, state, limit, offset));
    }

    public Pager<Warc> paginateWithState(long page, int stateId) {
        return new Pager<>(page, dao.countWarcsInState(stateId),
                (offset, limit) -> dao.paginateWarcsInState(stateId, offset, limit));
    }

    public Warc getOrNull(long id) {
        return dao.findWarc(id);
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
        return NotFoundException.check(dao.findWarcByFilename(filename), "warc with filename: " + filename, 0);
    }

    public String stateName(int stateId) {
        return dao.findWarcStateName(stateId);
    }

    @Deprecated
    public List<Warc> listAll() {
        return dao.listWarcs();
    }

    public void updateSha256(long warcId, String calculatedDigest) {
        dao.updateWarcSha256(warcId, calculatedDigest);
    }

    public List<Warc> findByState(int stateId, int limit) {
        return dao.findWarcsInState(stateId, limit);
    }

    public void updateState(long id, int stateId) {
        dao.updateWarcState(id, stateId)
    }
}
