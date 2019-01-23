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

        RecordStats stats = new RecordStats();
        Date time = new Date();
        stats.update(100, time);
        warcs.updateRecordStats(crawlId, stats);

        Crawl crawl2 = crawls.get(crawlId);
        assertEquals(time, crawl2.getStartTime());
        assertEquals(time, crawl2.getEndTime());
    }
}
