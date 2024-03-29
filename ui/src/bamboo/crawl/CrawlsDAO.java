package bamboo.crawl;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.*;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;
import org.jdbi.v3.sqlobject.transaction.Transactional;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@RegisterRowMapper(CrawlsDAO.CrawlMapper.class)
@RegisterRowMapper(CrawlsDAO.CrawlWithSeriesNameMapper.class)
@RegisterRowMapper(CrawlsDAO.ArtifactMapper.class)
@RegisterRowMapper(WarcsDAO.StatisticsMapper.class)
@RegisterRowMapper(CrawlsDAO.LanguageStatsMapper.class)
public interface CrawlsDAO extends Transactional<CrawlsDAO> {
    class CrawlMapper implements RowMapper<Crawl> {
        @Override
        public Crawl map(ResultSet rs, StatementContext ctx) throws SQLException {
            return new Crawl(rs);
        }
    }

    class CrawlWithSeriesNameMapper implements RowMapper<CrawlAndSeriesName> {
        @Override
        public CrawlAndSeriesName map(ResultSet rs, StatementContext ctx) throws SQLException {
            return new CrawlAndSeriesName(rs);
        }
    }

    class ArtifactMapper implements RowMapper<Artifact> {
        @Override
        public Artifact map(ResultSet rs, StatementContext ctx) throws SQLException {
            return new Artifact(rs);
        }
    }

    class LanguageStatsMapper implements RowMapper<Map.Entry<String,Long>> {

        @Override
        public Map.Entry<String, Long> map(ResultSet rs, StatementContext ctx) throws SQLException {
            return new AbstractMap.SimpleEntry<>(rs.getString(1), rs.getLong(2));
        }
    }

    @CreateSqlObject
    WarcsDAO warcs();

    @SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state, creator) VALUES (:name, :crawl_series_id, :state, :creator)")
    @GetGeneratedKeys
    long createCrawl(@Bind("name") String name, @Bind("crawl_series_id") Long crawlSeriesId, @Bind("state") int state, @Bind("creator") String creator);

    @SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state, pandas_instance_id) VALUES (:name, :crawl_series_id, :state, :pandasInstanceId)")
    @GetGeneratedKeys
    long createPandasCrawlInternal(@Bind("name") String name, @Bind("crawl_series_id") Long crawlSeriesId, @Bind("state") int state, @Bind("pandasInstanceId") long pandasInstanceId);

    @SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state, pandas_instance_id, creator) VALUES (:name, :crawlSeriesId, :state, :pandasInstanceId, :creator)")
    @GetGeneratedKeys
    long createCrawl(@BindBean Crawl crawl);

    @SqlQuery("SELECT * FROM crawl WHERE id = :id")
    Crawl findCrawl(@Bind("id") long crawlId);

    @SqlQuery("SELECT * FROM crawl WHERE pandas_instance_id = :instanceId")
    Crawl findCrawlByPandasInstanceId(@Bind("instanceId") long instanceId);

    @SqlQuery("SELECT * FROM crawl WHERE webrecorder_collection_id = :collectionId")
    Crawl findCrawlByWebrecorderCollectionId(@Bind("collectionId") String collectionId);

    @SqlQuery("SELECT * FROM crawl WHERE crawl_series_id = :crawl_series_id ORDER BY id DESC")
    List<Crawl> findCrawlsByCrawlSeriesId(@Bind("crawl_series_id") long crawlSeriesId);

    @SqlQuery("SELECT * FROM crawl WHERE state = :state")
    List<Crawl> findCrawlsByState(@Bind("state") int state);

    @SqlQuery("SELECT id FROM crawl WHERE pandas_instance_id IS NOT NULL AND id > :start ORDER BY id ASC LIMIT 100")
    List<Long> listPandasCrawlIds(@Bind("start") long start);

    @SqlUpdate("UPDATE crawl SET path = :path WHERE id = :id")
    int updateCrawlPath(@Bind("id") long id, @Bind("path") String path);

    @SqlUpdate("UPDATE crawl SET state = :state WHERE id = :crawlId")
    int updateCrawlState(@Bind("crawlId") long crawlId, @Bind("state") int state);

    @SqlUpdate("UPDATE crawl SET name = :name, description = :description, modifier = :modifier, modified = NOW() WHERE id = :crawlId")
    int updateCrawl(@Bind("crawlId") long crawlId, @Bind("name") String name, @Bind("description") String description, @Bind("modifier") String modifier);

    @SqlUpdate("UPDATE crawl SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
    int incrementRecordStatsForCrawl(@Bind("id") long crawlId, @Bind("records") long records, @Bind("bytes") long bytes);

    @SqlQuery("SELECT crawl.*, crawl_series.name FROM crawl LEFT JOIN crawl_series ON crawl.crawl_series_id = crawl_series.id ORDER BY crawl.end_time DESC, crawl.id DESC LIMIT :limit OFFSET :offset")
    List<CrawlAndSeriesName> paginateCrawlsWithSeriesName(@Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT COUNT(*) FROM crawl")
    long countCrawls();

    @SqlQuery("SELECT * FROM crawl LEFT JOIN crawl_series ON crawl.crawl_series_id = crawl_series.id WHERE crawl_series_id = :seriesId ORDER BY crawl.end_time DESC, crawl.id DESC LIMIT :limit OFFSET :offset")
    List<Crawl> paginateCrawlsWithSeriesId(@Bind("seriesId") long seriesId, @Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT COUNT(*) FROM crawl WHERE crawl_series_id = :seriesId")
    long countCrawlsWithSeriesId(@Bind("seriesId") long seriesId);

    @SqlUpdate("UPDATE crawl SET warc_files = (SELECT COALESCE(COUNT(*), 0) FROM warc WHERE warc.crawl_id = crawl.id AND warc.warc_state_id <> " + Warc.DELETED + "),\n" +
            "warc_size = (SELECT COALESCE(SUM(size), 0) FROM warc WHERE warc.crawl_id = crawl.id AND warc.warc_state_id <> " + Warc.DELETED + "),\n" +
            "records = (SELECT COALESCE(SUM(records), 0) FROM warc WHERE warc.crawl_id = crawl.id AND warc.warc_state_id <> " + Warc.DELETED + "),\n" +
            "record_bytes = (SELECT COALESCE(SUM(record_bytes), 0) FROM warc WHERE warc.crawl_id = crawl.id AND warc.warc_state_id <> " + Warc.DELETED + ")")
    int recalculateWarcStats();

    @SqlUpdate("INSERT INTO artifact (crawl_id, type, path, size, sha256, relpath) VALUES (:crawl_id, :type, :path, :size, :sha256, :relpath)")
    @GetGeneratedKeys
    long createArtifact(@Bind("crawl_id") long crawlId, @Bind("type") String type, @Bind("path") Path path, @Bind("size") long size, @Bind("sha256") String sha256, @Bind("relpath") String relpath);

    @SqlBatch("INSERT INTO artifact (crawl_id, type, path, relpath, size, sha256, blob_id) VALUES (:crawlId, :artifact.type, :artifact.path, :artifact.relpath, :artifact.size, :artifact.sha256, :artifact.blobId)")
    void batchInsertArtifacts(@Bind("crawlId") long crawlId, @BindBean("artifact") Iterator<Artifact> artifacts);

    @SqlQuery("SELECT count(*) FROM artifact WHERE crawl_id = :crawlId")
    long getArtifactCount(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT sum(size) FROM artifact WHERE crawl_id = :crawlId")
    long getArtifactBytes(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT * FROM artifact WHERE crawl_id = :crawlId")
    List<Artifact> listArtifacts(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT * FROM artifact WHERE id = :artifactId LIMIT 1")
    Artifact findArtifact(@Bind("artifactId") long artifactId);

    @SqlQuery("SELECT * FROM artifact WHERE crawl_id = :crawlId AND relpath = :relpath LIMIT 1")
    Artifact findArtifactByRelpath(@Bind("crawlId") long crawlId, @Bind("relpath") String relpath);

    @SqlQuery("SELECT * FROM artifact WHERE crawl_id = :crawlId AND relpath LIKE :pattern")
    List<Artifact> findArtifactsByRelpathLike(@Bind("crawlId") long crawlId, @Bind("pattern") String pattern);

    @SqlQuery("SELECT COUNT(*) totalFiles, SUM(size) totalSize, 0 totalRecords FROM artifact")
    Statistics getArtifactStatistics();

    @SqlUpdate("DELETE FROM crawl_language_stats WHERE crawl_id = :crawlId")
    void deleteLanguageStats(@Bind("crawlId") long crawlId);

    @SqlBatch("INSERT INTO crawl_language_stats (crawl_id, language, pages) VALUES (:crawlId, :key, :value)")
    void batchInsertLanguageStats(@Bind("crawlId") long crawlId, @BindBean Set<Map.Entry<String, Long>> languageStats);

    @Transaction
    default void replaceCrawlLanguageStats(long crawlId, Map<String, Long> stats) {
        deleteLanguageStats(crawlId);
        batchInsertLanguageStats(crawlId, stats.entrySet());
    }

    @SqlQuery("SELECT language, pages FROM crawl_language_stats WHERE crawl_id = :crawlId ORDER BY pages DESC")
    List<Map.Entry<String, Long>> getLanguageStats(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT COUNT(*) FROM crawl_language_stats WHERE crawl_id = :crawlId")
    long countLanguages(@Bind("crawlId") long crawlId);
}
