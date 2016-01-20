package bamboo.crawl;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

@RegisterMapper(WarcsDAO.WarcMapper.class)
public interface WarcsDAO {
    class WarcMapper implements ResultSetMapper<Warc> {
        @Override
        public Warc map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Warc(resultSet);
        }
    }

    interface InsertableWarc {
        long getStateId();
        String getPath();
        String getFilename();
        long getSize();
        String getSha256();
    }

    @Transaction
    default long insertWarc(long crawlId, int stateId, String path, String filename, long size, String sha256) {
        incrementWarcStatsForCrawl(crawlId, 1, size);
        long warcId = insertWarcWithoutRollup(crawlId, stateId, path, filename, size, sha256);
        insertWarcHistory(warcId, stateId);
        return warcId;
    }

    default void incrementWarcStatsForCrawl(long crawlId, int warcFilesDelta, long sizeDelta) {
        incrementWarcStatsForCrawlInternal(crawlId, warcFilesDelta, sizeDelta);
        incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, warcFilesDelta, sizeDelta);
    }

    @Transaction
    default int updateWarc(long crawlId, long warcId, int stateId, String path, String filename, long oldSize, long size, String sha256) {
        int rows = updateWarcWithoutRollup(warcId, stateId, path, filename, size, sha256);
        if (rows > 0) {
            insertWarcHistory(warcId, stateId);
            incrementWarcStatsForCrawl(crawlId, 0, size - oldSize);
        }
        return rows;
    }

    @Transaction
    default int updateWarcSize(long crawlId, long warcId, long oldSize, long size) {
        int rows = updateWarcSizeWithoutRollup(warcId, size);
        if (rows > 0) {
            incrementWarcStatsForCrawl(crawlId, 0, size - oldSize);
        }
        return rows;
    }

    @Transaction
    default int updateWarcState(long warcId, int stateId) {
        int rows = updateWarcStateWithoutHistory(warcId, stateId);
        if (rows > 0) {
            insertWarcHistory(warcId, stateId);
        }
        return rows;
    }

    @SqlUpdate("UPDATE warc SET warc_state_id = :stateId, path = :path, filename = :filename, size = :size, sha256 = :sha256 WHERE id = :warcId")
    int updateWarcWithoutRollup(@Bind("warcId") long warcId, @Bind("stateId") int stateId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size, @Bind("sha256") String sha256);

    @SqlUpdate("UPDATE crawl_series SET warc_files = warc_files + :warc_files,  warc_size = warc_size + :warc_size WHERE id = (SELECT crawl_series_id FROM crawl WHERE crawl.id = :crawl_id)")
    void incrementWarcStatsForCrawlSeriesByCrawlId(@Bind("crawl_id") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

    @SqlUpdate("UPDATE crawl SET warc_files = warc_files + :warc_files, warc_size = warc_size + :warc_size WHERE id = :crawlId")
    void incrementWarcStatsForCrawlInternal(@Bind("crawlId") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

    @SqlUpdate("INSERT INTO warc (crawl_id, path, filename, size, warc_state_id, sha256) VALUES (:crawlId, :path, :filename, :size, :stateId, :sha256)")
    @GetGeneratedKeys
    long insertWarcWithoutRollup(@Bind("crawlId") long crawlId, @Bind("stateId") int stateId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size, @Bind("sha256") String sha256);

    @SqlBatch("INSERT INTO warc (crawl_id, path, filename, size, warc_state_id, sha256) VALUES (:crawlId, :warc.path, :warc.filename, :warc.size, :warc.stateId, :warc.sha256)")
    void batchInsertWarcsWithoutRollup(@Bind("crawlId") long crawlId, @BindBean("warc") Iterator<Warc> warcs);

    @Deprecated
    @SqlQuery("SELECT * FROM warc")
    List<Warc> listWarcs();

    @SqlQuery("SELECT * FROM warc WHERE id = :warcId")
    Warc findWarc(@Bind("warcId") long warcId);

    @SqlQuery("SELECT * FROM warc WHERE filename = :filename")
    Warc findWarcByFilename(@Bind("filename") String filename);

    @SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId")
    List<Warc> findWarcsByCrawlId(@Bind("crawlId") long crawlId);

    @SqlQuery("SELECT * FROM warc WHERE warc_state_id = :stateId LIMIT :limit")
    List<Warc> findWarcsInState(@Bind("stateId") int stateId, @Bind("limit") int limit);

    @SqlQuery("SELECT COUNT(*) FROM warc WHERE warc_state_id = :stateId")
    long countWarcsInState(@Bind("stateId") int stateId);

    @SqlQuery("SELECT COUNT(*) FROM WARC WHERE crawl_id = :crawlId")
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

    @SqlUpdate("UPDATE warc SET records = :records, record_bytes = :record_bytes WHERE id = :id")
    int updateWarcRecordStats(@Bind("id") long warcId, @Bind("records") long records, @Bind("record_bytes") long recordBytes);

    @SqlUpdate("UPDATE warc SET size = :size WHERE id = :id")
    int updateWarcSizeWithoutRollup(@Bind("id") long warcId, @Bind("size") long size);

    @SqlUpdate("UPDATE warc SET sha256 = :digest WHERE id = :id")
    int updateWarcSha256(@Bind("id") long id, @Bind("digest") String digest);

    @SqlQuery("SELECT name FROM warc_state WHERE id = :stateId")
    String findWarcStateName(@Bind("stateId") int stateId);
}
