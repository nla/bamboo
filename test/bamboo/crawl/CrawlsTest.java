package bamboo.crawl;

import bamboo.core.Fixtures;
import bamboo.util.Pager;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
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
        Crawls crawls = new Crawls(fixtures.dao.crawls(), new Serieses(fixtures.dao.serieses()), new Warcs(fixtures.dao.warcs()));

        Crawl crawlMD = new Crawl();
        crawlMD.setCrawlSeriesId(fixtures.crawlSeriesId);
        crawlMD.setName("balloon");
        crawlMD.setPandasInstanceId(42L);

        List<Path> warcs = Arrays.asList(
                tmp.newFile("test.warc.gz").toPath(),
                tmp.newFile("test2.warc.gz").toPath());

        long id = crawls.createInPlace(crawlMD, warcs);

        {
            Crawl crawl = crawls.get(id);
            assertEquals("balloon", crawl.getName());
            assertEquals(42L, crawl.getPandasInstanceId().longValue());
            assertEquals(fixtures.crawlSeriesId, crawl.getCrawlSeriesId().longValue());
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
