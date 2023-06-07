package bamboo.crawl;

import bamboo.core.Fixtures;
import bamboo.util.Pager;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
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
        Crawls crawls = new Crawls(fixtures.dao.crawls(), serieses, new Warcs(fixtures.dao.warcs()), null);

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

        crawls.stats(id);
    }


    private static void writeTarEntry(TarArchiveOutputStream tar, String name, byte[] contents) throws IOException {
        TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(contents.length);
        tar.putArchiveEntry(entry);
        tar.write(contents);
        tar.closeArchiveEntry();
    }
    @Test
    public void testLanguageStats() throws IOException {
        Serieses serieses = new Serieses(fixtures.dao.serieses());
        Crawls crawls = new Crawls(fixtures.dao.crawls(), serieses, new Warcs(fixtures.dao.warcs()), null);
        Crawl crawl = new Crawl();
        crawl.setName("language test");

        var tarFile = tmp.newFile("test-language-buckets.tar.gz");
        try (var tar = new TarArchiveOutputStream(new GZIPOutputStream(new FileOutputStream(tarFile)))) {
            writeTarEntry(tar, "buckets/en.txt", "1\n2\n3\n".getBytes(UTF_8));
            writeTarEntry(tar, "buckets/kr.txt", "1\n2\n".getBytes(UTF_8));
        }
        long crawlId = crawls.createInPlace(crawl, List.of(), List.of(tarFile.toPath()));
        crawls.refreshLanguageStats(crawlId);
        System.out.println(fixtures.dao.crawls().getLanguageStats(crawlId));
    }

}
