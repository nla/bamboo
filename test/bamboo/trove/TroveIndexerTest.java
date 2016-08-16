package bamboo.trove;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import bamboo.task.Document;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.WarcProgressManager;
import bamboo.trove.demand.OnDemandWarcManager;
import bamboo.trove.services.BambooRestrictionService.FilterSegments;
import bamboo.util.SurtFilter;
import bamboo.util.Urls;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.archive.url.SURT;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * *
 * Make sure the Trove indexer can understand Bamboo data and is running off a correct understanding of how
 * Bamboo handles restrictions.
 */
public class TroveIndexerTest {
  private static ObjectMapper jsonMapper;
  private static JavaType type;
  private static List<Document> warc79;
  private static List<Document> warc127555;

  @BeforeClass
  public static void init() throws ClassNotFoundException, IOException {
    jsonMapper = new ObjectMapper();
    Class<?> clz = Class.forName(Document.class.getCanonicalName());
    type = jsonMapper.getTypeFactory().constructCollectionType(List.class, clz);
    warc79 = getWarcResource(79);
    warc127555 = getWarcResource(127555);
  }

  private static List<Document> getWarcResource(long warcId) throws IOException {
    InputStream is = TroveIndexerTest.class.getResourceAsStream("/warc" + warcId + ".json");
    String jsonData = IOUtils.toString(is, "UTF-8");
    return jsonMapper.readValue(jsonData, type);
  }

  @Test
  public void surtTests() {
    // Random samples through warc #127555
    assertEquals("SURT did not transform as expected (50)",
            "(au,com,cosmeticdentistryaustralia,)/robots.txt", getSurt(50));
    assertEquals("SURT did not transform as expected (100)",
            "(au,com,cosmeticsurgerynewcastle,", getSurt(100));
    assertEquals("SURT did not transform as expected (150)",
            "(au,com,coreessentials,)/our-services/medx-spine-care/research", getSurt(150));
    assertEquals("SURT did not transform as expected (200)",
            "(au,com,cosworthaustralia,)/robots.txt", getSurt(200));
    assertEquals("SURT did not transform as expected (250)",
            "(au,com,concretingmildura,)/category/concrete-floor", getSurt(250));
    assertEquals("SURT did not transform as expected (300)",
            "(au,com,conceptfinancial,)/about-us/our-philosophy/", getSurt(300));
    assertEquals("SURT did not transform as expected (350)",
            "(au,com,corinnemakeupdesign,)/prices.htm", getSurt(350));
  }

  // SURT : Sort-friendly URI Reordering Transform
  private String getSurt(int docOffset) {
    return SURT.toSURT(Urls.removeScheme(warc127555.get(docOffset).getUrl()));
  }

  @Test
  public void simpleFilterTests() {
    String ACCEPT_ALL = "+";
    String REJECT_ALL = "-";
    String ACCEPT_AU = "+(au,";
    String REJECT_AU = "-(au,";
    String ACCEPT_COM = "+(au,com,";
    String REJECT_COM = "-(au,com,";
    String ACCEPT_HOST = "+(au,com,cosmeticdentistryaustralia,";
    String REJECT_HOST = "-(au,com,cosmeticdentistryaustralia,";
    String ACCEPT_ROBOTS = "+(au,com,cosmeticdentistryaustralia,)/robots.txt";
    String REJECT_ROBOTS = "-(au,com,cosmeticdentistryaustralia,)/robots.txt";
    // TODO: wildcards? SurtFilter has none

    String surt = getSurt(50); //"(au,com,cosmeticdentistryaustralia,)/robots.txt"

    //*************
    // Accept cases
    // 1) Because we accept everything
    assertTrue(makeFilter(ACCEPT_ALL).accepts(surt));
    // 2) Because we accept AU
    assertTrue(makeFilter(REJECT_ALL, ACCEPT_AU).accepts(surt));
    // 3) Because we accept COM
    assertTrue(makeFilter(REJECT_ALL, REJECT_AU, ACCEPT_COM).accepts(surt));
    // 4) Because we accept this specific host
    assertTrue(makeFilter(REJECT_ALL, REJECT_AU, REJECT_COM, ACCEPT_HOST).accepts(surt));
    // 5) Because the filename is desired
    assertTrue(makeFilter(REJECT_ALL, REJECT_AU, REJECT_COM, REJECT_HOST, ACCEPT_ROBOTS).accepts(surt));
    // 6) Simple ordering test
    // TODO: In discussion with Alex it was noted that specificity is more important than order.
    // Doesn't look that way though. Looks like the last line that 'hits' decides the return value
    //assertTrue(makeFilter(ACCEPT_ROBOTS, REJECT_ALL, REJECT_AU, REJECT_COM, REJECT_HOST).accepts(surt));

    //*************
    // Reject cases
    // 1) Because we reject everything
    assertFalse(makeFilter(REJECT_ALL).accepts(surt));
    // 2) Because we reject AU
    assertFalse(makeFilter(ACCEPT_ALL, REJECT_AU).accepts(surt));
    // 3) Because we reject COM
    assertFalse(makeFilter(ACCEPT_ALL, ACCEPT_AU, REJECT_COM).accepts(surt));
    // 4) Because we reject this specific host
    assertFalse(makeFilter(ACCEPT_ALL, ACCEPT_AU, ACCEPT_COM, REJECT_HOST).accepts(surt));
    // 5) Because the filename is undesirable
    assertFalse(makeFilter(ACCEPT_ALL, ACCEPT_AU, ACCEPT_COM, ACCEPT_HOST, REJECT_ROBOTS).accepts(surt));
    // 6) Simple ordering test
    // TODO: As above
    //assertTrue(makeFilter(REJECT_ROBOTS, ACCEPT_ALL, ACCEPT_AU, ACCEPT_COM, ACCEPT_HOST).accepts(surt));
  }

  private SurtFilter makeFilter(String... filterStrings) {
    return new SurtFilter(StringUtils.join(filterStrings, "\n"));
  }

  @Test
  public void filterSegmentsTest() throws IOException {
    // TODO parsing and util testing on segments
    // We might not even do this... it is for the nightly process to
    // find content that should match a new incoming restriction segment
    FilterSegments segment;
  }

  @Test
  public void iCanParseDocumentLists() throws IOException {
    // Example 1
    assertEquals("Document list length is not correct", 224, warc79.size());
    for (Document doc : warc79) {
      assertEquals("Unexpected status code", 200, doc.getStatusCode());
      assertEquals("Unexpected content type", "text/html", doc.getContentType());
    }

    // Example 2 - An abridged warc. The full one has nearly 12k URLs
    assertEquals("Document list length is not correct", 352, warc127555.size());
    int c200 = 0, c301 = 0, c302 = 0, c303 = 0, c403 = 0, c404 = 0, mimeHtml = 0, mimePdf = 0;
    for (Document doc : warc127555) {
      if (doc.getStatusCode() == 200) {
        c200++;
      } else if (doc.getStatusCode() == 301) {
        c301++;
      } else if (doc.getStatusCode() == 302) {
        c302++;
      } else if (doc.getStatusCode() == 303) {
        c303++;
      } else if (doc.getStatusCode() == 403) {
        c403++;
      } else if (doc.getStatusCode() == 404) {
        c404++;
      } else {
        assertEquals("Unexpected status code", 200, doc.getStatusCode());
      }

      if ("text/html".equals(doc.getContentType())) {
        mimeHtml++;
      } else if ("application/pdf".equals(doc.getContentType())) {
        mimePdf++;
      } else {
        assertEquals("Unexpected content type", "text/html", doc.getContentType());
      }
    }
    assertEquals("Unexpected status codes (200)", 261, c200);
    assertEquals("Unexpected status codes (301)", 15, c301);
    assertEquals("Unexpected status codes (302)", 18, c302);
    assertEquals("Unexpected status codes (303)", 1, c303);
    assertEquals("Unexpected status codes (403)", 9, c403);
    assertEquals("Unexpected status codes (404)", 48, c404);
    assertEquals("Status count and row count should match", warc127555.size(),
            c200 + c301 + c302 + c303 + c403 + c404);
    assertEquals("Unexpected content type (html)", 342, mimeHtml);
    assertEquals("Unexpected content type (pdf)", 10, mimePdf);
    assertEquals("Content count and row count should match", warc127555.size(), mimeHtml + mimePdf);
  }

  @Test
  public void connectToDevelViaDomain() throws InterruptedException {
    String metricsName = "serverMetrics";
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(metricsName);

    OnDemandWarcManager domain = new OnDemandWarcManager();
    domain.forTestSetBambooBaseUrl("http://heritrix-devel.nla.gov.au/bamboo/warcs/");
    domain.forTestSetMetricsRegistryName(metricsName);

    // Confirm stats before
    Timer t = metrics.timer("bambooReadTimer");
    assertEquals("Timer should not have data before first request", 0, t.getCount());

    WarcProgressManager batch79 = domain.getWarcFromBamboo(79);
    assertEquals("Warc 79 has wrong document count", 224, batch79.size());
    assertEquals("Timer did not increment", 1, t.getCount());
    domain.enqueueBatch(batch79);

    // Rush through the queues... keep in mind that there is (up to) a 1s lag on the batch state tracking
    assertBatchState(batch79, false, false, false);
    // Filter queue
    IndexerDocument doc = domain.getNextFilterJob();
    t = metrics.timer("filterTimer");
    while (doc != null) {
      doc.filter.start(t);
      doc.filter.finish();
      doc = domain.getNextFilterJob();
    }
    assertEquals("Timer count is wrong", 224, t.getCount());
    //Thread.sleep(1100);
    //assertBatchState(batch79, true, false, false);

    // Transform queue
    doc = domain.getNextTransformJob();
    t = metrics.timer("transformTimer");
    while (doc != null) {
      doc.transform.start(t);
      doc.transform.finish();
      doc = domain.getNextTransformJob();
    }
    assertEquals("Timer count is wrong", 224, t.getCount());
    //Thread.sleep(1100);
    //assertBatchState(batch79, true, true, false);

    // Index queue
    doc = domain.getNextIndexJob();
    t = metrics.timer("indexTimer");
    while (doc != null) {
      doc.index.start(t);
      doc.index.finish();
      doc = domain.getNextIndexJob();
    }
    assertEquals("Timer count is wrong", 224, t.getCount());
    Thread.sleep(1100);
    assertBatchState(batch79, true, true, true);
  }

  private void assertBatchState(WarcProgressManager batch79, boolean filterDone, boolean transformDone, boolean indexDone) {
    if (!filterDone) {
      assertFalse("Filter should not have finished", batch79.isFilterComplete());
    } else {
      assertTrue("Filter should have finished", batch79.isFilterComplete());
    }
    if (!transformDone) {
      assertFalse("Transform should not have finished", batch79.isTransformComplete());
    } else {
      assertTrue("Transform should have finished", batch79.isTransformComplete());
    }
    if (!indexDone) {
      assertFalse("Index should not have finished", batch79.isIndexComplete());
    } else {
      assertTrue("Index should have finished", batch79.isIndexComplete());
    }
  }

  @Test
  public void indexerDocumentTest() {
    // Basic lifecycle testing
    Timer timer = new Timer();
    IndexerDocument document = null;

    // Constructor
    try {
      document = new IndexerDocument(1234, null);
      fail("IllegalArgumentException should have been thrown by null parameter in constructor");
    } catch (IllegalArgumentException ex) {
      assertTrue("Constructor should have failed", document == null);
    }

    Document doc = new Document();
    doc.setWarcOffset(4321);
    document = new IndexerDocument(1234, doc);
    assertNotNull("Constructor and getter don't line up", document.getBambooDocument());
    assertEquals("Invalid docId generated", "1234/4321", document.getDocId());

    // Filtering
    startWithAssertions(document.filter, timer);
    document.applyFiltering(DocumentStatus.RESTRICTED, ContentThreshold.DOCUMENT_START_ONLY);
    document.filter.finish();

    // Working
    startWithAssertions(document.transform, timer);
    document.transform.finish();
    
    // Writing
    startWithAssertions(document.index, timer);
    document.index.finish();
  }

  private void startWithAssertions(IndexerDocument.StateTracker state, Timer timer) {
    confirmIllegalStateToStop(state);
    state.start(timer);
    confirmIllegalStateToStart(state);
  }

  private void confirmIllegalStateToStop(IndexerDocument.StateTracker state) {
    try {
      state.finish();
      fail("IllegalStateException should have been thrown by trying to stop");
    } catch (IllegalStateException ex) {
    }
  }
  private void confirmIllegalStateToStart(IndexerDocument.StateTracker state) {
    try {
      state.start(null);
      fail("IllegalStateException should have been thrown by trying to start");
    } catch (IllegalStateException ex) {
    }
  }
}