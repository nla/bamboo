package bamboo.core;

import bamboo.util.Pager;

import java.util.List;

public class Collections {
    private final DbPool dbPool;

    public Collections(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public List<Db.CollectionWithFilters> listWhereSeriesId(long seriesId) {
        try (Db db = dbPool.take()) {
            return db.listCollectionsForCrawlSeries(seriesId);
        }
    }

    public List<Collection> listAll() {
        try (Db db = dbPool.take()) {
            return db.listCollections();
        }
    }

    public Collection getOrNull(long id) {
        try (Db db = dbPool.take()) {
            return db.findCollection(id);
        }
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
        try (Db db = dbPool.take()) {
            return new Pager<>(page, db.countCollections(), db::paginateCollections);
        }
    }

    public long create(Collection collection) {
        try (Db db = dbPool.take()) {
            return db.createCollection(collection);
        }
    }

    public void update(long collectionId, Collection collection) {
        try (Db db = dbPool.take()) {
            int rows = db.updateCollection(collectionId, collection);
            if (rows == 0) {
                throw new NotFoundException("collection", collectionId);
            }
        }
    }
}
