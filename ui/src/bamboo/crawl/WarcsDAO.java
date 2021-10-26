package bamboo.crawl;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

@RegisterMapper({WarcsDAO.WarcMapper.class, WarcsDAO.CollectionWarcMapper.class, WarcsDAO.WarcResumptionTokenMapper.class,
    WarcsDAO.StatisticsMapper.class})
public interface WarcsDAO extends Transactional<WarcsDAO> {

    class WarcMapper implements ResultSetMapper<Warc> {
        @Override
        public Warc map(int i, ResultSet rs, StatementContext statementContext) throws SQLException {
            Warc warc = new Warc();
            warc.setId(rs.getLong("id"));
            warc.setCrawlId(rs.getLong("crawl_id"));
            warc.setStateId(rs.getInt("warc_state_id"));
            String path = rs.getString("path");
            warc.setPath(path == null ? null : Paths.get(path));
            warc.setSize(rs.getLong("size"));
            warc.setRecords(rs.getLong("records"));
            warc.setRecordBytes(rs.getLong("record_bytes"));
            warc.setFilename(rs.getString("filename"));
            warc.setSha256(rs.getString("sha256"));
            warc.setBlobId((Long)rs.getObject("blob_id"));
            warc.setStartTime((Date)rs.getObject("start_time"));
            warc.setEndTime((Date)rs.getObject("end_time"));
            return warc;
        }
    }

    interface InsertableWarc {
        long getStateId();
        String getPath();
        String getFilename();
        long getSize();
        String getSha256();
    }

    public class StatisticsMapper implements ResultSetMapper<Statistics> {
        @Override
        public Statistics map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new Statistics(
                    r.getLong("totalFiles"),
                    r.getLong("totalSize"),
                    r.getLong("totalRecords"));
        }
    }

    @SqlQuery("SELECT COUNT(*) totalFiles, SUM(size) totalSize, SUM(records) totalRecords FROM warc WHERE warc_state_id <> -4")
    Statistics getStatistics();

    @SqlUpdate("UPDATE warc SET warc_state_id = :stateId, path = :path, filename = :filename, size = :size, sha256 = :sha256 WHERE id = :warcId")
    int updateWarcWithoutRollup(@Bind("warcId") long warcId, @Bind("stateId") int stateId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size, @Bind("sha256") String sha256);

    @SqlUpdate("UPDATE crawl_series SET warc_files = warc_files + :warc_files,  warc_size = warc_size + :warc_size WHERE id = (SELECT crawl_series_id FROM crawl WHERE crawl.id = :crawl_id)")
    void incrementWarcStatsForCrawlSeriesByCrawlId(@Bind("crawl_id") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

    @SqlUpdate("UPDATE crawl SET warc_files = warc_files + :warc_files, warc_size = warc_size + :warc_size WHERE id = :crawlId")
    void incrementWarcStatsForCrawlInternal(@Bind("crawlId") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

    @SqlUpdate("INSERT INTO warc (crawl_id, path, filename, size, warc_state_id, sha256) VALUES (:crawlId, :path, :filename, :size, :stateId, :sha256)")
    @GetGeneratedKeys
    long insertWarcWithoutRollup(@Bind("crawlId") long crawlId, @Bind("stateId") int stateId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size, @Bind("sha256") String sha256);

    @SqlBatch("INSERT INTO warc (crawl_id, path, filename, size, warc_state_id, sha256, blob_id) VALUES (:crawlId, :warc.path, :warc.filename, :warc.size, :warc.stateId, :warc.sha256, :warc.blobId)")
    void batchInsertWarcsWithoutRollup(@Bind("crawlId") long crawlId, @BindBean("warc") Iterator<Warc> warcs);

    @Deprecated
    @SqlQuery("SELECT * FROM warc")
    List<Warc> listWarcs();

    @SqlQuery("SELECT * FROM warc WHERE id > :fromId LIMIT :limit")
    List<Warc> streamWarcs(@Bind("fromId") long fromId, @Bind("limit") int limit);

    @SqlQuery("SELECT * FROM warc " +
            "LEFT JOIN crawl ON warc.crawl_id = crawl.id " +
            "WHERE warc.id > :fromId AND " +
            "crawl.crawl_series_id = :seriesId " +
            "LIMIT :limit")
    List<Warc> streamWarcsInSeries(@Bind("fromId") long fromId, @Bind("seriesId") long seriesId, @Bind("limit") int limit);

    @SqlQuery("SELECT * FROM warc WHERE id = :warcId FOR UPDATE")
    Warc selectForUpdate(@Bind("warcId") long warcId);

    @SqlQuery("SELECT * FROM warc WHERE id = :warcId")
    Warc findWarc(@Bind("warcId") long warcId);

    @SqlQuery("SELECT * FROM warc LIMIT 1")
    Warc findAnyWarc();

    @SqlQuery("SELECT * FROM warc WHERE filename = :filename")
    Warc findWarcByFilename(@Bind("filename") String filename);

    @SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId AND filename = :filename")
    Warc findWarcByCrawlIdAndFilename(@Bind("crawlId") long crawlId, @Bind("filename") String filename);

    @SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId")
    List<Warc> findWarcsByCrawlId(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT * FROM warc WHERE warc_state_id = :stateId LIMIT :limit")
    List<Warc> findWarcsInState(@Bind("stateId") int stateId, @Bind("limit") int limit);

    @SqlQuery("SELECT COUNT(*) FROM warc WHERE warc_state_id = :stateId")
    long countWarcsInState(@Bind("stateId") int stateId);

    @SqlQuery("SELECT COUNT(*) FROM warc WHERE crawl_id = :crawlId")
    long countWarcsWithCrawlId(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT * FROM warc WHERE warc_state_id = :stateId LIMIT :limit OFFSET :offset")
    List<Warc> paginateWarcsInState(@Bind("stateId") int stateId, @Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId ORDER BY filename LIMIT :limit OFFSET :offset")
    List<Warc> paginateWarcsInCrawl(@Bind("crawlId") long crawlId, @Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId AND warc_state_id = :stateId LIMIT :limit OFFSET :offset")
    List<Warc> paginateWarcsInCrawlAndState(@Bind("crawlId") long crawlId, @Bind("stateId") int stateId, @Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT COUNT(*) FROM warc WHERE crawl_id = :it AND warc_state_id = :stateId")
    long countWarcsInCrawlAndState(@Bind long crawlId, @Bind("stateId") int stateId);

    @SqlUpdate("UPDATE warc SET warc_state_id = :stateId WHERE id = :warcId")
    int updateWarcStateWithoutHistory(@Bind("warcId") long warcId, @Bind("stateId") int stateId);

    @SqlUpdate("INSERT INTO warc_history (warc_id, warc_state_id) VALUES (:warcId, :stateId)")
    int insertWarcHistory(@Bind("warcId") long warcId, @Bind("stateId") int stateId);

    @SqlUpdate("UPDATE warc SET records = :stats.records, record_bytes = :stats.recordBytes, start_time = :stats.startTime, end_time = :stats.endTime WHERE id = :id")
    int updateWarcRecordStats(@Bind("id") long warcId, @BindBean("stats") RecordStats stats);

    @SqlUpdate("UPDATE warc SET size = :size WHERE id = :id")
    int updateWarcSizeWithoutRollup(@Bind("id") long warcId, @Bind("size") long size);

    @SqlUpdate("UPDATE warc SET sha256 = :digest WHERE id = :id")
    int updateWarcSha256(@Bind("id") long id, @Bind("digest") String digest);

    @SqlUpdate("UPDATE warc SET blob_id = :blobId WHERE id = :id")
    int updateWarcBlobId(@Bind("id") long id, @Bind("blobId") Long blobId);

    @SqlUpdate("UPDATE warc SET path = :path WHERE id = :id")
    int updateWarcPath(@Bind("id") long id, @Bind("path") String path);

    @SqlQuery("SELECT name FROM warc_state WHERE id = :stateId")
    String findWarcStateName(@Bind("stateId") int stateId);


    @SqlUpdate(
            "UPDATE crawl SET " +
            "  records = records + :records - (SELECT records FROM warc WHERE id = :warcId), " +
            "  record_bytes = record_bytes + :recordBytes - (SELECT record_bytes FROM warc WHERE id = :warcId), " +
            "  start_time = COALESCE(LEAST(start_time, :startTime), :startTime), " +
            "  end_time = COALESCE(GREATEST(end_time, :endTime), :endTime) " +
            "WHERE id = (SELECT crawl_id FROM warc WHERE id = :warcId)")
    int updateRecordStatsRollupForCrawl(@Bind("warcId") long warcId, @Bind("records") long records,
                                        @Bind("recordBytes") long recordBytes, @Bind("startTime") Date startTime,
                                        @Bind("endTime") Date endTime);

    @SqlUpdate(
            "UPDATE crawl_series SET " +
            "  records = records + :records - (SELECT records FROM warc WHERE id = :warcId), " +
            "  record_bytes = record_bytes + :recordBytes - (SELECT record_bytes FROM warc WHERE id = :warcId) " +
            "WHERE id = (SELECT crawl.crawl_series_id FROM crawl INNER JOIN warc ON warc.crawl_id = crawl.id WHERE warc.id = :warcId)")
    int updateRecordStatsRollupForSeries(@Bind("warcId") long warcId, @Bind("records") long records,
                                         @Bind("recordBytes") long recordBytes, @Bind("startTime") Date startTime,
                                         @Bind("endTime") Date endTime);

    class CollectionWarcMapper implements ResultSetMapper<CollectionWarc> {
        @Override
        public CollectionWarc map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            return new CollectionWarc(r);
        }
    }

    @SqlQuery("SELECT * FROM collection_warc WHERE collection_id = :collectionId AND warc_id = :warcId")
    CollectionWarc findCollectionWarc(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId);

    @SqlQuery("SELECT * FROM collection_warc WHERE collection_id = :collectionId AND warc_id = :warcId FOR UPDATE")
    CollectionWarc selectCollectionWarcForUpdate(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId);

    @SqlUpdate("DELETE FROM collection_warc WHERE collection_id = :collectionId AND warc_id = :warcId")
    int deleteCollectionWarc(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId);

    @SqlUpdate("INSERT INTO collection_warc (collection_id, warc_id, records, record_bytes) VALUES (:collectionId, :warcId, :records, :recordBytes)")
    void insertCollectionWarc(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId, @Bind("records") long records, @Bind("recordBytes") long recordBytes);

    @SqlUpdate("UPDATE collection SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
    int incrementRecordStatsForCollection(@Bind("id") long collectionId, @Bind("records") long records, @Bind("bytes") long bytes);

    @SqlQuery("SELECT warc.* FROM warc\n" +
            "LEFT JOIN crawl ON warc.crawl_id = crawl.id\n" +
            "LEFT JOIN crawl_series ON crawl.crawl_series_id = crawl_series.id\n" +
            "LEFT JOIN collection_series ON crawl_series.id = collection_series.crawl_series_id\n" +
            "WHERE collection_series.collection_id = :collectionId\n" +
            "  AND warc.id >= :start\n" +
            "ORDER BY warc.id asc LIMIT :rows")
    List<Warc> findByCollectionId(@Bind("collectionId") long collectionId, @Bind("start") long start, @Bind("rows") long rows);


    @SqlQuery("SELECT wh.time, wh.warc_id, w.records urlCount\n" +
            "FROM warc_history wh\n" +
            "         LEFT JOIN warc w ON w.id = wh.warc_id\n" +
            "         LEFT JOIN crawl ON w.crawl_id = crawl.id\n" +
            "         LEFT JOIN collection_series cs ON cs.crawl_series_id = crawl.crawl_series_id\n" +
            "         LEFT JOIN warc_history more_recent\n" +
            "                   ON wh.warc_id = more_recent.warc_id\n" +
            "                       AND more_recent.id > wh.id\n" +
            "WHERE more_recent.id IS NULL\n" +
            "  AND (wh.time, wh.warc_id) > (:afterTime, :afterWarcId) AND\n" +
            "    wh.warc_state_id >= :stateAtLeast\n" +
            "  AND cs.collection_id = :collectionId\n" +
            "ORDER BY wh.time ASC, wh.warc_id ASC\n" +
            "LIMIT :limit")
    List<WarcResumptionToken> resumptionByCollectionIdAndStateId(
            @Bind("collectionId") long collectionId,
            @Bind("stateAtLeast") int stateAtLeast,
            @Bind("afterTime") Timestamp afterTime,
            @Bind("afterWarcId") long warcId,
            @Bind("limit") int limit);

    class WarcResumptionTokenMapper implements ResultSetMapper<WarcResumptionToken> {
        @Override
        public WarcResumptionToken map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new WarcResumptionToken(resultSet.getTimestamp("time").toInstant(),
                    resultSet.getLong("warc_id"),
                    resultSet.getLong("urlCount"));
        }
    }
}
