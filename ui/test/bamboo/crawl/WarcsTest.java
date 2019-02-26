package bamboo.crawl;

import bamboo.core.Fixtures;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class WarcsTest {

    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    @ClassRule
    public static TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testUpdateRecordStats() throws IOException {
        Warcs warcs = new Warcs(fixtures.dao.warcs());
        Serieses serieses = new Serieses(fixtures.dao.serieses());
        Crawls crawls = new Crawls(fixtures.dao.crawls(), serieses, warcs);

        Path testFile = tmp.newFile("test.warc.gz").toPath();

        Crawl crawl = new Crawl();
        crawl.setName("test crawl");
        long crawlId = crawls.createInPlace(crawl, Arrays.asList(testFile));
        long warcId = warcs.findByCrawlId(crawlId).get(0).getId();

        RecordStats stats = new RecordStats();
        Date time = new Date();
        stats.update(100, time);
        warcs.updateRecordStats(warcId, stats);

        Warc warc = warcs.get(warcId);
        assertEquals(100, warc.getRecordBytes());
        assertEquals(time, warc.getStartTime());
        assertEquals(time, warc.getEndTime());

        Crawl crawl2 = crawls.get(crawlId);
        assertEquals(100, crawl2.getRecordBytes());
        assertEquals(time, crawl2.getStartTime());
        assertEquals(time, crawl2.getEndTime());
    }
}
