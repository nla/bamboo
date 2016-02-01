package bamboo.crawl;

import bamboo.core.Fixtures;
import bamboo.util.Pager;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CrawlsTest {

    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testCRUD() throws IOException {
        Serieses serieses = new Serieses(fixtures.dao.serieses());
        Crawls crawls = new Crawls(fixtures.dao.crawls(), serieses, new Warcs(fixtures.dao.warcs()));

        Series series = new Series();
        series.setName("test series");
        series.setPath(Paths.get("/tmp/test"));
        long seriesId = serieses.create(series);

        Crawl crawlMD = new Crawl();
        crawlMD.setCrawlSeriesId(seriesId);
        crawlMD.setName("balloon");
        crawlMD.setPandasInstanceId(48L);

        List<Path> warcs = Arrays.asList(
                tmp.newFile("test.warc.gz").toPath(),
                tmp.newFile("test2.warc.gz").toPath());

        long id = crawls.createInPlace(crawlMD, warcs);

        {
            Crawl crawl = crawls.get(id);
            assertEquals("balloon", crawl.getName());
            assertEquals(48L, crawl.getPandasInstanceId().longValue());
            assertEquals(seriesId, crawl.getCrawlSeriesId().longValue());
        }

        crawls.update(id, "bubble", null);

        {
            Crawl crawl = crawls.get(id);
            assertEquals("bubble", crawl.getName());
        }

        Pager pager = crawls.pager(1);
        assertTrue(pager.totalItems > 0);
    }

}
