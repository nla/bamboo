package bamboo.virus;

import bamboo.core.Fixtures;
import bamboo.crawl.Crawl;
import bamboo.crawl.Crawls;
import bamboo.crawl.Series;
import bamboo.crawl.Serieses;
import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VirusScanDAOTest {
    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    @Test
    public void findingsAndProblemsAreUpsertedInsteadOfAddedPerRun() throws Exception {
        Serieses serieses = new Serieses(fixtures.dao.serieses());
        Warcs warcs = new Warcs(fixtures.dao.warcs());
        Crawls crawls = new Crawls(fixtures.dao.crawls(), serieses, warcs, null);

        Series series = new Series();
        series.setName("virus scan test");
        long seriesId = serieses.create(series);
        Crawl crawl = new Crawl();
        crawl.setName("virus scan test");
        crawl.setCrawlSeriesId(seriesId);
        long crawlId = crawls.createInPlace(crawl, List.of());
        long warcId = fixtures.dao.warcs().insertWarcWithoutRollup(crawlId, Warc.IMPORTED, null,
                "test.warc", 1, null, null);

        VirusScanDAO dao = fixtures.dao.virusScans();
        Timestamp now = Timestamp.from(Instant.now());
        long runId = dao.createRun(warcId, 1, "ClamAV test", now);
        VirusFinding finding = new VirusFinding(warcId, 10, "urn:uuid:test", "https://example.org/",
                Instant.now(), "application/octet-stream", "sha1:TEST", "Test.Signature");
        dao.insertFinding(runId, finding, Timestamp.from(finding.captureTime()), now);
        assertEquals(1, dao.updateFinding(runId, finding, Timestamp.from(finding.captureTime()), now));

        VirusScanProblem problem = new VirusScanProblem(warcId, 10, "limit", "too large");
        dao.insertProblem(runId, problem, now);
        assertEquals(1, dao.updateProblem(runId, problem, now));

        assertEquals(1L, fixtures.dbPool.dbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM virus_finding").mapTo(Long.class).one()).longValue());
        assertEquals(2L, fixtures.dbPool.dbi.withHandle(handle ->
                handle.createQuery("SELECT detection_count FROM virus_finding").mapTo(Long.class).one()).longValue());
        assertEquals(1L, fixtures.dbPool.dbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM virus_scan_problem").mapTo(Long.class).one()).longValue());
        assertEquals(2L, fixtures.dbPool.dbi.withHandle(handle ->
                handle.createQuery("SELECT attempts FROM virus_scan_problem").mapTo(Long.class).one()).longValue());
    }
}
