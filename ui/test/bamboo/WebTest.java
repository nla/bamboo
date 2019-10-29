package bamboo;

import bamboo.app.Bamboo;
import bamboo.crawl.Collection;
import bamboo.crawl.Crawl;
import bamboo.crawl.Series;
import bamboo.crawl.Warc;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@Import(AppTestConfig.class)
@SpringBootTest
@AutoConfigureMockMvc
public class WebTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private Bamboo bamboo;

    @Test
    public void test() throws Exception {
        mockMvc.perform(get("/")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls")).andExpect(status().isOk());
    }

    @Test
    public void testCollections() throws Exception {
        mockMvc.perform(get("/collections")).andExpect(status().isOk());
        mockMvc.perform(get("/collections/new")).andExpect(status().isOk());
        String testCollection = mockMvc.perform(post("/collections/new")
                .param("name", "test collection")
                .param("description", "test description"))
                .andExpect(status().is3xxRedirection())
                .andReturn().getResponse().getHeader("Location");
        mockMvc.perform(get(testCollection))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("test description")));
        mockMvc.perform(get(testCollection + "/edit"))
                .andExpect(status().isOk());
        mockMvc.perform(post(testCollection + "/edit")
                .param("description", "new description"))
                .andExpect(status().is3xxRedirection());
        mockMvc.perform(get(testCollection))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("new description")));
    }

    @Test
    public void testSeries() throws Exception {
        mockMvc.perform(get("/series")).andExpect(status().isOk());
        mockMvc.perform(get("/series/new")).andExpect(status().isOk());
        String testSeries = mockMvc.perform(post("/series/new")
                .param("name", "test series")
                .param("description", "test description"))
                .andExpect(status().is3xxRedirection())
                .andReturn().getResponse().getHeader("Location");
        mockMvc.perform(get(testSeries))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("test description")));
        mockMvc.perform(get(testSeries + "/edit"))
                .andExpect(status().isOk());
        mockMvc.perform(post(testSeries + "/edit")
                .param("description", "new description"))
                .andExpect(status().is3xxRedirection());
        mockMvc.perform(get(testSeries))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("new description")));
    }

    @Test
    public void testCrawlAndWarc() throws Exception {
        Path warcFile = folder.newFile("example.warc.gz").toPath();
        try (InputStream stream = getClass().getResourceAsStream("/bamboo/task/example.warc.gz")) {
            Files.copy(stream, warcFile, REPLACE_EXISTING);
        }

        Collection collection = new Collection();
        collection.setName("Test collection");
        long collectionId = bamboo.collections.create(collection);

        Series series = new Series();
        series.setName("Test series");
        long seriesId = bamboo.serieses.create(series);
        bamboo.serieses.update(seriesId, series, Arrays.asList(collectionId), Arrays.asList(""));

        Crawl crawl = new Crawl();
        crawl.setName("Test crawl");
        crawl.setCrawlSeriesId(seriesId);
        long crawlId = bamboo.crawls.createInPlace(crawl, Arrays.asList(warcFile));
        Warc warc = bamboo.warcs.findByCrawlId(crawlId).get(0);

        mockMvc.perform(get("/crawls/" + crawlId)).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/warcs")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/artifacts")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/edit")).andExpect(status().isOk());
        mockMvc.perform(post("/crawls/" + crawlId + "/edit").param("name", crawl.getName())
                .param("description", "test description")).andExpect(status().is3xxRedirection());
        mockMvc.perform(get("/crawls/" + crawlId)).andExpect(status().isOk())
                .andExpect(content().string(containsString("test description")));

        mockMvc.perform(get("/warcs/" + warc.getId() + "/details")).andExpect(status().isOk());
        mockMvc.perform(get("/warcs/" + warc.getId() + "/cdx")).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.TEXT_PLAIN))
                .andExpect(content().string(containsString("- 20161116220655 http://www-test.nla.gov.au/xinq/presentations/abstract.html text/html 200 387f5ef1511fe47bf91ca9fdcf6c41511fc3e480 - - 2756 339 example.warc.gz")));
        mockMvc.perform(get("/warcs/" + warc.getId() + "/text")).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().string(containsString("Search and browse tool")));
        mockMvc.perform(get("/warcs/" + warc.getId())).andExpect(status().isOk())
                .andExpect(content().contentType("application/warc"));
        mockMvc.perform(get("/warcs/" + warc.getId()).header("Range", "bytes=1-10"))
                .andExpect(status().isPartialContent()).andExpect(content().contentType("application/warc"))
                .andExpect(header().longValue("Content-Length", 10))
                .andExpect(header().string("Content-Range", "1-10/6574"));

        // tests below rely on the warc having been indexed
        mockMvc.perform(post("/warcs/" + warc.getId() + "/reindex")).andExpect(status().isOk())
                .andExpect(content().string(containsString("CDX indexed")))
                .andExpect(content().contentType("text/plain;charset=UTF-8"));

        mockMvc.perform(get("/collections/" + collectionId + "/warcs/json"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().string(containsString("\"id\": " + warc.getId())));
        mockMvc.perform(get("/collections/" + collectionId + "/warcs/sync"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().string(containsString("\"id\": " + warc.getId())));
    }
}
