package bamboo.core;

import bamboo.util.Pager;

import java.util.List;

public class Serieses {
    private final DbPool dbPool;

    public Serieses(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public Series getOrNull(long id) {
        try (Db db = dbPool.take()) {
            return db.findCrawlSeriesById(id);
        }
    }
    /**
     * Retrieve a series's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Series get(long id) {
        return NotFoundException.check(getOrNull(id), "series", id);
    }

    public Pager<Db.CrawlSeriesWithCount> paginate(long page) {
        try (Db db = dbPool.take()) {
            return new Pager<>(page, db.countCrawlSeries(), db::paginateCrawlSeries);
        }
    }

    public long create(Series series) {
        try (Db db = dbPool.take()) {
            return db.createCrawlSeries(series.getName(), series.getPath().toString());
        }
    }

    public void update(long seriesId, Series series, List<Long> collectionIds, List<String> collectionUrlFilters) {
        try (Db db = dbPool.take()) {
            int rows = db.updateCrawlSeries(seriesId, series.getName(),
                    series.getPath() == null ? null : series.getPath().toString(),
                    series.getDescription(),
                    collectionIds, collectionUrlFilters);
            if (rows == 0) {
                throw new NotFoundException("series", seriesId);
            }
        }
    }

    public List<Series> listAll() {
        try (Db db = dbPool.take()) {
            return db.listCrawlSeries();
        }
    }

    public List<Series> listImportable() {
        try (Db db = dbPool.take()) {
            return db.listImportableCrawlSeries();
        }
    }
}
