package bamboo;

import bamboo.app.Bamboo;
import bamboo.crawl.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.test.context.support.WithSecurityContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class)
@AutoConfigureMockMvc
@WithMockBambooUser
public class WebTest {
    public static class Factory {

    }

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
                .param("name", "test collection")
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
                .param("name", "test series")
                .param("description", "new description"))
                .andExpect(status().is3xxRedirection());
        mockMvc.perform(get(testSeries))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("new description")));
        long seriesId = Long.parseLong(testSeries.replaceFirst(".*/", ""));
        Series series = bamboo.serieses.get(seriesId);
        assertNotNull(series.getCreated());
        assertNotNull(series.getModifier());
        assertEquals("mockuser", series.getCreator());
        assertEquals("mockuser", series.getModifier());
    }

    @Test
    public void testCrawlAndWarc() throws Exception {
        Collection collection = new Collection();
        collection.setName("Test collection");
        long collectionId = bamboo.collections.create(collection);

        Series series = new Series();
        series.setName("Test series");
        long seriesId = bamboo.serieses.create(series);
        bamboo.serieses.update(seriesId, series, Arrays.asList(collectionId), Arrays.asList(""));

        long crawlId;
        Warc warc;
        Crawl crawl;
        mockMvc.perform(get("/crawls/new")).andExpect(status().isOk());
        try (InputStream stream = getClass().getResourceAsStream("/bamboo/task/example.warc.gz");
             InputStream stream2 = getClass().getResourceAsStream("/bamboo/task/notfound.warc.gz");
             InputStream crawlLogStream  = getClass().getResourceAsStream("/bamboo/task/crawl.log")) {
            String crawlPath = mockMvc.perform(multipart("/crawls/new")
                    .file(new MockMultipartFile("warcFile", "example.warc.gz", "application/warc", stream))
                    .file(new MockMultipartFile("warcFile", "notfound.warc.gz", "application/warc", stream2))
                    .file(new MockMultipartFile("artifact", "crawl.log", "text/plain", crawlLogStream))
                    .param("name", "Test crawl")
                    .param("crawlSeriesId", Long.toString(seriesId)))
                    .andExpect(status().is3xxRedirection())
                    .andReturn().getResponse().getHeader("Location");
            crawlId = Long.parseLong(crawlPath.replaceFirst(".*/", ""));
            crawl = bamboo.crawls.get(crawlId);
            List<Warc> warcs = bamboo.warcs.findByCrawlId(crawlId);
            assertEquals(2, warcs.size());
            warc = warcs.get(0);
            assertNotNull(warc.getBlobId());
            assertEquals("example.warc.gz", warc.getFilename());
            assertEquals("3069a7432667c18be49028abb385c9cc2a9ad8ba8d8962db9083c01a8f01859a", warc.getSha256());
            Warc warc2 = warcs.get(1);
            assertNotNull(warc2.getBlobId());
            assertNotEquals(warc.getBlobId(), warc2.getBlobId());
            assertEquals("notfound.warc.gz", warc2.getFilename());
            assertEquals("e2a9ca11af67b7724db65042611127a18e642c6870f052a5826ed6aa8adad872", warc2.getSha256());
            List<Artifact> artifacts = bamboo.crawls.listArtifacts(crawlId);
            assertEquals(1, artifacts.size());
            Artifact artifact = artifacts.get(0);
            assertEquals("LOG", artifact.getType());
            assertEquals("9841e00ddc894fc6456e668b032733e2390e580d5afb32f17ee37ad8331963c9", artifact.getSha256());
        }

        mockMvc.perform(get("/crawls")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/warcs/upload")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/warcs/upload?crawlSeries=" + seriesId)).andExpect(status().isOk());
        try (InputStream stream = getClass().getResourceAsStream("/bamboo/task/example.warc.gz")) {
            mockMvc.perform(multipart("/crawls/" + crawlId + "/warcs/upload")
                    .file(new MockMultipartFile("warcFile", "example2.warc.gz", "application/warc", stream)))
                    .andExpect(status().is3xxRedirection());
            List<Warc> warcs = bamboo.warcs.findByCrawlId(crawlId);
            assertEquals(3, warcs.size());
        }

        mockMvc.perform(get("/crawls/" + crawlId)).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/warcs")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/artifacts")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls/" + crawlId + "/edit")).andExpect(status().isOk());
        mockMvc.perform(post("/crawls/" + crawlId + "/edit").param("name", crawl.getName())
                .param("description", "test description")).andExpect(status().is3xxRedirection());
        mockMvc.perform(get("/crawls/" + crawlId)).andExpect(status().isOk())
                .andExpect(content().string(containsString("test description")));
        mockMvc.perform(get("/crawls")).andExpect(status().isOk());

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
//        mockMvc.perform(post("/warcs/" + warc.getId() + "/reindex")).andExpect(status().isOk())
//                .andExpect(content().string(containsString("CDX indexed")))
//                .andExpect(content().contentType("text/plain;charset=UTF-8"));
//
//        mockMvc.perform(get("/collections/" + collectionId + "/warcs/json"))
//                .andExpect(status().isOk())
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
//                .andExpect(content().string(containsString("\"id\": " + warc.getId())));
//        mockMvc.perform(get("/collections/" + collectionId + "/warcs/sync"))
//                .andExpect(status().isOk())
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
//                .andExpect(content().string(containsString("\"id\": " + warc.getId())));
    }
}
