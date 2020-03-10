package bamboo.crawl;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

@RegisterMapper({SeriesDAO.CrawlSeriesMapper.class, SeriesDAO.CrawlSeriesWithCountMapper.class})
public interface SeriesDAO {

    class CrawlSeriesWithCount extends Series {
        public final long crawlCount;

        public CrawlSeriesWithCount(ResultSet rs) throws SQLException {
            super(rs);
            crawlCount = rs.getLong("crawl_count");
        }
    }

    class CrawlSeriesMapper implements ResultSetMapper<Series> {
        @Override
        public Series map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new Series(r);
        }
    }

    class CrawlSeriesWithCountMapper implements ResultSetMapper<CrawlSeriesWithCount> {
        @Override
        public CrawlSeriesWithCount map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new CrawlSeriesWithCount(r);
        }
    }

    @SqlQuery("SELECT * FROM crawl_series WHERE id = :id")
    Series findCrawlSeriesById(@Bind("id") long crawlSeriesId);

    @SqlQuery("SELECT * FROM crawl_series ORDER BY name")
    List<Series> listCrawlSeries();

    @SqlQuery("SELECT * FROM crawl_series WHERE path IS NOT NULL ORDER BY name")
    List<Series> listImportableCrawlSeries();

    @SqlQuery("SELECT COUNT(*) FROM crawl_series")
    long countCrawlSeries();

    @SqlQuery("SELECT *, (SELECT COUNT(*) FROM crawl WHERE crawl_series_id = crawl_series.id) crawl_count FROM crawl_series ORDER BY name LIMIT :limit OFFSET :offset")
    List<CrawlSeriesWithCount> paginateCrawlSeries(@Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT COUNT(*) FROM crawl_series WHERE agency_id = :agencyId")
    long countCrawlSeriesForAgencyId(@Bind("agencyId") long agencyId);

    @SqlQuery("SELECT *, (SELECT COUNT(*) FROM crawl WHERE crawl_series_id = crawl_series.id) crawl_count FROM crawl_series WHERE agency_id = :agencyId ORDER BY name LIMIT :limit OFFSET :offset")
    List<CrawlSeriesWithCount> paginateCrawlSeriesForAgencyId(@Bind("agencyId") long agencyId, @Bind("limit") long limit, @Bind("offset") long offset);

    @SqlUpdate("UPDATE crawl_series SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
    int incrementRecordStatsForCrawlSeries(@Bind("id") long crawlSeriesId, @Bind("records") long records, @Bind("bytes") long bytes);

    @SqlUpdate("INSERT INTO crawl_series (name, path, description, creator) VALUES (:name, :path, :description, :creator)")
    @GetGeneratedKeys
    long createCrawlSeries(@Bind("name") String name, @Bind("path") Path path, @Bind("description") String description, @Bind("creator") String creator);

    @SqlUpdate("UPDATE crawl_series SET name = :name, path = :path, description = :description, modifier = :modifier, modified = NOW() WHERE id = :id")
    int updateCrawlSeries(@Bind("id") long seriesId, @Bind("name") String name, @Bind("path") String path, @Bind("description") String description, @Bind("modifier") String modifier);

    @SqlUpdate("UPDATE crawl_series SET warc_files = (SELECT COALESCE(SUM(warc_files), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), warc_size = (SELECT COALESCE(SUM(warc_size), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), records = (SELECT COALESCE(SUM(records), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), record_bytes = (SELECT COALESCE(SUM(record_bytes), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id)")
    int recalculateWarcStats();

    @SqlUpdate("DELETE FROM collection_series WHERE crawl_series_id = :it")
    void removeCrawlSeriesFromAllCollections(@Bind long crawlSeriesId);

    @SqlBatch("INSERT INTO collection_series (crawl_series_id, collection_id, url_filters) VALUES (:crawl_series_id, :collection_id, :url_filters)")
    void addCrawlSeriesToCollections(@Bind("crawl_series_id") long crawlSeriesId, @Bind("collection_id") List<Long> collectionIds, @Bind("url_filters") List<String> urlFilters);
}
