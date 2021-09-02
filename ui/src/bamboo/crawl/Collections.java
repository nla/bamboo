package bamboo.crawl;

import bamboo.core.DbPool;
import bamboo.core.NotFoundException;
import bamboo.util.Pager;

import java.util.List;

public class Collections {
    private final CollectionsDAO dao;

    public Collections(CollectionsDAO dao) {
        this.dao = dao;
    }

    public List<Collection> listWhereSeriesId(long seriesId) {
        return dao.listCollectionsForCrawlSeries(seriesId);
    }

    public List<Collection> listAll() {
        return dao.listCollections();
    }

    public Collection getOrNull(long id) {
        return dao.findCollection(id);
    }

    /**
     * Retrieve a series's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Collection get(long id) {
        return NotFoundException.check(getOrNull(id), "collection", id);
    }

    public Pager<Collection> paginate(long page) {
        return new Pager<>(page, dao.countCollections(), dao::paginateCollections);
    }

    public long create(Collection collection) {
        return dao.createCollection(collection);
    }

    public void update(long collectionId, Collection collection) {
        int rows = dao.updateCollection(collectionId, collection);
        if (rows == 0) {
            throw new NotFoundException("collection", collectionId);
        }
    }


    public List<Collection> findByCrawlSeriesId(long crawlSeriesId) {
        return dao.listCollectionsForCrawlSeries(crawlSeriesId);
    }
}
