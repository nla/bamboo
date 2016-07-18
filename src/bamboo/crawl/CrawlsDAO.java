package bamboo.crawl;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@RegisterMapper({CrawlsDAO.CrawlMapper.class, CrawlsDAO.CrawlWithSeriesNameMapper.class})
public interface CrawlsDAO extends Transactional<CrawlsDAO> {

    class CrawlMapper implements ResultSetMapper<Crawl> {
        @Override
        public Crawl map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new Crawl(r);
        }
    }

    class CrawlWithSeriesNameMapper implements ResultSetMapper<CrawlAndSeriesName> {
        @Override
        public CrawlAndSeriesName map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new CrawlAndSeriesName(r);
        }
    }

    @CreateSqlObject
    WarcsDAO warcs();

    @SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state) VALUES (:name, :crawl_series_id, :state)")
    @GetGeneratedKeys
    long createCrawl(@Bind("name") String name, @Bind("crawl_series_id") Long crawlSeriesId, @Bind("state") int state);

    @SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state, pandas_instance_id) VALUES (:name, :crawl_series_id, :state, :pandasInstanceId)")
    @GetGeneratedKeys
    long createPandasCrawlInternal(@Bind("name") String name, @Bind("crawl_series_id") Long crawlSeriesId, @Bind("state") int state, @Bind("pandasInstanceId") long pandasInstanceId);

    @SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state, pandas_instance_id) VALUES (:name, :crawlSeriesId, :state, :pandasInstanceId)")
    @GetGeneratedKeys
    long createCrawl(@BindBean Crawl crawl);

    @SqlQuery("SELECT * FROM crawl WHERE id = :id")
    Crawl findCrawl(@Bind("id") long crawlId);

    @SqlQuery("SELECT * FROM crawl WHERE pandas_instance_id = :instanceId")
    Crawl findCrawlByPandasInstanceId(@Bind("instanceId") long instanceId);

    @SqlQuery("SELECT * FROM crawl WHERE crawl_series_id = :crawl_series_id ORDER BY id DESC")
    List<Crawl> findCrawlsByCrawlSeriesId(@Bind("crawl_series_id") long crawlSeriesId);

    @SqlQuery("SELECT * FROM crawl WHERE state = :state")
    List<Crawl> findCrawlsByState(@Bind("state") int state);

    @SqlUpdate("UPDATE crawl SET path = :path WHERE id = :id")
    int updateCrawlPath(@Bind("id") long id, @Bind("path") String path);

    @SqlUpdate("UPDATE crawl SET state = :state WHERE id = :crawlId")
    int updateCrawlState(@Bind("crawlId") long crawlId, @Bind("state") int state);

    @SqlUpdate("UPDATE crawl SET name = :name, description = :description WHERE id = :crawlId")
    int updateCrawl(@Bind("crawlId") long crawlId, @Bind("name") String name, @Bind("description") String description);

    @SqlUpdate("UPDATE crawl SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
    int incrementRecordStatsForCrawl(@Bind("id") long crawlId, @Bind("records") long records, @Bind("bytes") long bytes);

    @SqlQuery("SELECT crawl.*, crawl_series.name FROM crawl LEFT JOIN crawl_series ON crawl.crawl_series_id = crawl_series.id ORDER BY crawl.end_time DESC, crawl.id DESC LIMIT :limit OFFSET :offset")
    List<CrawlAndSeriesName> paginateCrawlsWithSeriesName(@Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT COUNT(*) FROM crawl")
    long countCrawls();

    @SqlUpdate("UPDATE crawl SET warc_files = (SELECT COALESCE(COUNT(*), 0) FROM warc WHERE warc.crawl_id = crawl.id), warc_size = (SELECT COALESCE(SUM(size), 0) FROM warc WHERE warc.crawl_id = crawl.id), records = (SELECT COALESCE(SUM(records), 0) FROM warc WHERE warc.crawl_id = crawl.id), record_bytes = (SELECT COALESCE(SUM(record_bytes), 0) FROM warc WHERE warc.crawl_id = crawl.id)")
    int refreshWarcStatsOnCrawls();

    @SqlUpdate("INSERT INTO artifact (crawl_id, type, path, size, sha256) VALUES (:crawl_id, :type, :path, :size, :sha256)")
    @GetGeneratedKeys
    long createArtifact(@Bind("crawl_id") long crawlId, @Bind("type") String type, @Bind("path") Path path, @Bind("size") long size, @Bind("sha256") String sha256);
}
