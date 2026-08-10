package bamboo.virus;

import bamboo.crawl.Warc;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindMethods;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

@RegisterRowMapper(VirusScanDAO.RunMapper.class)
public interface VirusScanDAO extends Transactional<VirusScanDAO> {
    @SqlQuery("SELECT COALESCE(MAX(id), 0) FROM warc WHERE warc_state_id NOT IN (" + Warc.OPEN + ", " + Warc.DELETED + ")")
    long maxEligibleWarcId();

    @SqlQuery("SELECT COUNT(*) FROM warc WHERE id <= :maxWarcId AND warc_state_id NOT IN (" + Warc.OPEN + ", " + Warc.DELETED + ")")
    long countEligibleWarcs(@Bind("maxWarcId") long maxWarcId);

    @SqlQuery("SELECT * FROM virus_scan_run WHERE state = 'running' ORDER BY id DESC LIMIT 1")
    VirusScanRun findRunningRun();

    @SqlQuery("SELECT * FROM virus_scan_run WHERE finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1")
    VirusScanRun findLatestFinishedRun();

    @SqlUpdate("INSERT INTO virus_scan_run (state, max_warc_id, warcs_total, clamav_version, started_at, heartbeat_at) " +
            "VALUES ('running', :maxWarcId, :warcsTotal, :clamavVersion, :now, :now)")
    @GetGeneratedKeys
    long createRun(@Bind("maxWarcId") long maxWarcId, @Bind("warcsTotal") long warcsTotal,
                   @Bind("clamavVersion") String clamavVersion, @Bind("now") Timestamp now);

    @SqlQuery("SELECT * FROM virus_scan_run WHERE id = :id")
    VirusScanRun findRun(@Bind("id") long id);

    @SqlUpdate("UPDATE virus_scan_run SET current_warc_id = :warcId, heartbeat_at = :now WHERE id = :runId")
    void setCurrentWarc(@Bind("runId") long runId, @Bind("warcId") long warcId, @Bind("now") Timestamp now);

    @SqlUpdate("UPDATE virus_scan_run SET last_warc_id = :warcId, current_warc_id = NULL, " +
            "warcs_scanned = warcs_scanned + 1, records_scanned = records_scanned + :records, " +
            "bytes_scanned = bytes_scanned + :bytes, findings_count = findings_count + :findings, " +
            "errors_count = errors_count + :errors, heartbeat_at = :now WHERE id = :runId")
    void advanceRun(@Bind("runId") long runId, @Bind("warcId") long warcId,
                    @Bind("records") long records, @Bind("bytes") long bytes,
                    @Bind("findings") long findings, @Bind("errors") long errors,
                    @Bind("now") Timestamp now);

    @SqlUpdate("UPDATE virus_scan_run SET state = CASE WHEN errors_count = 0 THEN 'completed' ELSE 'completed_with_errors' END, " +
            "last_warc_id = max_warc_id, current_warc_id = NULL, finished_at = :now, heartbeat_at = :now WHERE id = :runId")
    void finishRun(@Bind("runId") long runId, @Bind("now") Timestamp now);

    @SqlUpdate("UPDATE virus_finding SET status = 'not_detected' WHERE warc_id = :warcId AND status = 'active'")
    void markFindingsNotDetected(@Bind("warcId") long warcId);

    @SqlUpdate("UPDATE virus_finding SET warc_record_id = :finding.warcRecordId, target_uri = :finding.targetUri, " +
            "capture_time = :captureTime, media_type = :finding.mediaType, payload_digest = :finding.payloadDigest, " +
            "status = 'active', last_detected_at = :now, last_detected_run_id = :runId, detection_count = detection_count + 1 " +
            "WHERE warc_id = :finding.warcId AND record_offset = :finding.recordOffset AND clam_signature = :finding.signature")
    int updateFinding(@Bind("runId") long runId, @BindMethods("finding") VirusFinding finding,
                      @Bind("captureTime") Timestamp captureTime, @Bind("now") Timestamp now);

    @SqlUpdate("INSERT INTO virus_finding (warc_id, record_offset, warc_record_id, target_uri, capture_time, media_type, " +
            "payload_digest, clam_signature, first_detected_run_id, last_detected_run_id, first_detected_at, last_detected_at) " +
            "VALUES (:finding.warcId, :finding.recordOffset, :finding.warcRecordId, :finding.targetUri, :captureTime, " +
            ":finding.mediaType, :finding.payloadDigest, :finding.signature, :runId, :runId, :now, :now)")
    void insertFinding(@Bind("runId") long runId, @BindMethods("finding") VirusFinding finding,
                       @Bind("captureTime") Timestamp captureTime, @Bind("now") Timestamp now);

    @SqlUpdate("DELETE FROM virus_scan_problem WHERE warc_id = :warcId")
    void deleteProblems(@Bind("warcId") long warcId);

    @SqlUpdate("UPDATE virus_scan_problem SET message = :problem.message, last_seen_at = :now, last_run_id = :runId, " +
            "attempts = attempts + 1 WHERE warc_id = :problem.warcId AND record_offset = :problem.recordOffset " +
            "AND error_type = :problem.type")
    int updateProblem(@Bind("runId") long runId, @BindMethods("problem") VirusScanProblem problem, @Bind("now") Timestamp now);

    @SqlUpdate("INSERT INTO virus_scan_problem (warc_id, record_offset, error_type, message, last_run_id, first_seen_at, last_seen_at) " +
            "VALUES (:problem.warcId, :problem.recordOffset, :problem.type, :problem.message, :runId, :now, :now)")
    void insertProblem(@Bind("runId") long runId, @BindMethods("problem") VirusScanProblem problem, @Bind("now") Timestamp now);

    class RunMapper implements RowMapper<VirusScanRun> {
        @Override
        public VirusScanRun map(ResultSet rs, StatementContext ctx) throws SQLException {
            return new VirusScanRun(rs);
        }
    }
}
