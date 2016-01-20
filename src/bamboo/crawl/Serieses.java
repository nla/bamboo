package bamboo.crawl;

import bamboo.core.DbPool;
import bamboo.core.NotFoundException;
import bamboo.util.Pager;

import java.util.List;

public class Serieses {
    private final SeriesDAO dao;

    public Serieses(DbPool dbPool) {
        this.dao = dbPool.dbi.onDemand(SeriesDAO.class);
    }

    public Series getOrNull(long id) {
        return dao.findCrawlSeriesById(id);
    }

    /**
     * Retrieve a series's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Series get(long id) {
        return NotFoundException.check(getOrNull(id), "series", id);
    }

    public Pager<SeriesDAO.CrawlSeriesWithCount> paginate(long page) {
        return new Pager<>(page, dao.countCrawlSeries(), dao::paginateCrawlSeries);
    }

    public long create(Series series) {
        return dao.createCrawlSeries(series.getName(), series.getPath().toString());
    }

    public void update(long seriesId, Series series, List<Long> collectionIds, List<String> collectionUrlFilters) {
        String path = series.getPath() == null ? null : series.getPath().toString();
        int rows1 = dao.updateCrawlSeries(seriesId, series.getName(), path, series.getDescription());
        if (rows1 > 0) {
            dao.removeCrawlSeriesFromAllCollections(seriesId);
            dao.addCrawlSeriesToCollections(seriesId, collectionIds, collectionUrlFilters);
        }
        int rows = rows1;
        if (rows == 0) {
            throw new NotFoundException("series", seriesId);
        }
    }

    public List<Series> listAll() {
        return dao.listCrawlSeries();
    }

    public List<Series> listImportable() {
        return dao.listImportableCrawlSeries();
    }
}
