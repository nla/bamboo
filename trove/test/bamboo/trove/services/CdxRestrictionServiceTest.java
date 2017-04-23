/*
 * Copyright 2016-2017 National Library of Australia
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bamboo.trove.services;

import bamboo.task.Document;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.common.cdx.CdxAccessControl;
import bamboo.trove.common.cdx.CdxAccessPolicy;
import bamboo.trove.common.cdx.CdxRule;
import bamboo.trove.common.cdx.RulesDiff;
import bamboo.trove.rule.RuleChangeUpdateManager;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.netpreserve.urlcanon.Canonicalizer;
import org.netpreserve.urlcanon.ParsedUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.json.Jackson2ObjectMapperFactoryBean;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CdxRestrictionServiceTest {
  private static final Logger log = LoggerFactory.getLogger(CdxRestrictionServiceTest.class);

  private static ObjectMapper jsonMapper;
  private static CdxAccessControl ruleSet;
  private static CdxAccessControl ruleSetSecondCopy;
  private static CdxRestrictionService service;
  private static final String BEGINNING_OF_INPUT_TOKEN = "\\A";
  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

  private static String getResource(String resourceName) throws IOException {
    try (InputStream is = CdxRestrictionServiceTest.class.getResourceAsStream("/restrictions/" + resourceName)) {
      Scanner scanner = new Scanner(is).useDelimiter(BEGINNING_OF_INPUT_TOKEN);
      return scanner.hasNext() ? scanner.next() : "";
    }
  }

  private static <T> List<T> getCdxObjectList(String resourceName, Class<T> clazz) throws IOException {
    JavaType type = jsonMapper.getTypeFactory().constructCollectionType(List.class, clazz);
    return jsonMapper.readValue(getResource(resourceName), type);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Jackson2ObjectMapperFactoryBean factoryBean = new Jackson2ObjectMapperFactoryBean();
    factoryBean.setSimpleDateFormat(DATE_FORMAT);
    factoryBean.setFeaturesToDisable(SerializationFeature.WRITE_NULL_MAP_VALUES);
    factoryBean.afterPropertiesSet();
    jsonMapper = factoryBean.getObject();

    // Need a testing date that makes sense in the context of the rules in the JSON file
    // Using rule 16 (2011-2016) has a wildcard URL
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
    Date testingDate = sdf.parse("2014-01-01T00:00:00+1100");

    List<CdxRule> rules = getCdxObjectList("rules.json", CdxRule.class);
    List<CdxAccessPolicy> policies = getCdxObjectList("policies.json", CdxAccessPolicy.class);
    ruleSet = new CdxAccessControl(policies, rules);
    assertEquals("Basic parse error reading jdk8 Period object", 70,
            ruleSet.getRules().get(1144L).getPeriod().getYears());

    List<CdxRule> rules2 = getCdxObjectList("rules.json", CdxRule.class);
    List<CdxAccessPolicy> policies2 = getCdxObjectList("policies.json", CdxAccessPolicy.class);
    ruleSetSecondCopy = new CdxAccessControl(policies2, rules2);

    service = new CdxRestrictionService();
    service.overwriteRulesForTesting(ruleSet, testingDate);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testFilterDocument() throws CdxRestrictionService.RulesOutOfDateException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

    Document doc = new Document();
    String BASE = "http://www.garnautreview.org.au/ca25734e0016a131/webobj/";

    // Rule 16) Restricted 2011-2016 by
    // http://www.garnautreview.org.au/ca25734e0016a131/webobj/d0841534generalsubmission-ajlester/*
    String MATCH_URL = BASE + "d0841534generalsubmission-ajlester/test.html";
    String NOT_MATCH_URL = BASE + "NOT/d0841534generalsubmission-ajlester/test.html";
    Date MATCH_DATE = sdf.parse("2013-01-01T00:00:00+1100");
    Date NOT_MATCH_DATE = sdf.parse("2011-01-01T00:00:00+1100");

    setUrl(doc, MATCH_URL);
    doc.setDate(MATCH_DATE);
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());

    setUrl(doc, NOT_MATCH_URL);
    doc.setDate(MATCH_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());

    setUrl(doc, MATCH_URL);
    doc.setDate(NOT_MATCH_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());

    // Rule 17) A copy of rule 16 to test embargoes. URL pattern changed to
    // http://www.garnautreview.org.au/FAKE/ca25734e0016a131/webobj/d0841534generalsubmission-ajlester/*
    MATCH_URL = BASE + "FAKE/d0841534generalsubmission-ajlester/test.html";
    NOT_MATCH_URL = BASE + "FAKE/NOT/d0841534generalsubmission-ajlester/test.html";
    // Remember our fake 'today' is    "2014-01-01T00:00:00+1100" and the embargo is 1 year
    Date EMARGOED_DATE    = sdf.parse("2013-06-01T00:00:00+1100");
    Date NO_EMARGOED_DATE = sdf.parse("2012-06-01T00:00:00+1100");

    setUrl(doc, MATCH_URL);
    doc.setDate(EMARGOED_DATE);
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());

    setUrl(doc, MATCH_URL);
    doc.setDate(NO_EMARGOED_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());

    setUrl(doc, NOT_MATCH_URL);
    doc.setDate(EMARGOED_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());

    // Rule 18) Further evolves rule 17 by adding access dates between 2014-2015...
    // it is a stupid rule you wouldn't typically see in practice
    MATCH_URL = BASE + "STUPID/d0841534generalsubmission-ajlester/test.html";
    // Fake 'today' is still "2014-01-01T00:00:00+1100", so this rule will not apply
    // (access is outside the window) despite every other condition being applicable.
    setUrl(doc, MATCH_URL);
    doc.setDate(EMARGOED_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());
    // Now nudge it over the line by 1 day and it should be blocked
    Date testingDate = sdf.parse("2014-01-02T00:00:00+1100");
    service.overwriteRulesForTesting(null, testingDate);
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());
  }

  @Test
  public void testSchemaDoesNotMatter() throws CdxRestrictionService.RulesOutOfDateException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

    // Use Rule 16 again
    Document doc = new Document();
    doc.setDate(sdf.parse("2013-01-01T00:00:00+1100"));


    setUrl(doc, "http://www.garnautreview.org.au/ca25734e0016a131/webobj/d0841534generalsubmission-ajlester/test.html");
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());

    setUrl(doc, "https://www.garnautreview.org.au/ca25734e0016a131/webobj/d0841534generalsubmission-ajlester/test.html");
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());
  }

  @Test
  public void testMultiPatternMatches() throws CdxRestrictionService.RulesOutOfDateException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

    Document doc = new Document();
    setUrl(doc, "http://www.smh.com.au/randomstuff");

    // Rule 953) Multiple URL patterns from a Pandora conversion. 2nd is a broad wildcard for a whole domain
    // http://www.smh.com.au/*
    // Embargoed by 30 days
    // Fake 'today' is =>           "2014-01-01T00:00:00+1100"
    Date MATCH_DATE     = sdf.parse("2013-12-02T00:00:00+1100"); // - 30 days
    Date NOT_MATCH_DATE = sdf.parse("2013-12-01T00:00:00+1100"); // - 31 days

    doc.setDate(MATCH_DATE);
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());

    doc.setDate(NOT_MATCH_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());
  }

  @Test
  public void testRule1144() throws CdxRestrictionService.RulesOutOfDateException, ParseException {
    // Rule 1144 was just odd. has a left-anchored wildcard plus an empty string.
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

    Document doc = new Document();
    setUrl(doc, "http://blocked.adultshop.com.au");

    // Rule 953) Multiple URL patterns from a Pandora conversion. 2nd is a broad wildcard for a whole domain
    // *.adultshop.com.au
    // Embargoed for 70 years!!!
    // Fake 'today' is =>           "2014-01-01T00:00:00+1100"
    Date MATCH_DATE     = sdf.parse("1945-01-01T00:00:00+1100"); // - 69 years
    Date NOT_MATCH_DATE = sdf.parse("1943-01-01T00:00:00+1100"); // - 71 years

    doc.setDate(MATCH_DATE);
    assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, service.filterDocument(doc).getIndexerPolicy());

    doc.setDate(NOT_MATCH_DATE);
    assertEquals(DocumentStatus.ACCEPTED, service.filterDocument(doc).getIndexerPolicy());

    // Let's eyeball what that empty String will do
    RuleChangeUpdateManager manager = new RuleChangeUpdateManager();
    SolrQuery query = manager.convertRuleToSearch(ruleSet.getRules().get(1144L), "");
    boolean found = false;
    // We do not want to see *:* on the end
    String target = "(" + SolrEnum.URL_TOKENIZED + ":\"pandora.nla.gov.au/pan/35531/*\") OR ("
            + SolrEnum.URL_TOKENIZED + ":\"*.adultshop.com.au\")";
    for (String fq : query.getFilterQueries()) {
      if (target.equals(fq)) {
        found = true;
      }
    }
    if (!found) {
      // This behaviour is only ok when a single URL pattern was provided and it was empty.
      Assert.fail("An empty URL pattern should not be part of a query when there are other valid patterns");
    }
  }

  @Test
  public void testMatchesShittyUrls() throws CdxRestrictionService.RulesOutOfDateException {
    // These are simply URLs we have seen crash older implementations (like the SURT library)
    Date capture = new Date(System.currentTimeMillis() - 50000);
    Document doc = new Document();
    doc.setDate(capture);

    assertAccepted(doc, "/losch.com.au/favicon.ico");
    assertAccepted(doc, "http://mailto:linda@losch.com.au/favicon.ico");
    assertAccepted(doc, "http://mailto:gary.court@gmail.com/robots.txt");
    assertAccepted(doc, "http://user:pass@example.com/robots.txt");
    assertAccepted(doc, "http://user:pass@example.com/");
    assertAccepted(doc, "http://chrstian+1@cluebeck.de/");

    // Silly URLs, but there is a matching silly rule to confirm these will actually block, even though they have guff
    assertRestricted(doc, "http://mailto:linda@losch.bob.au/favicon.ico");
    assertRestricted(doc, "http://mailto:gary.court@gmail.bob.au/robots.txt");
    assertRestricted(doc, "http://user:pass@example.bob.au/robots.txt");
    assertRestricted(doc, "http://user:pass@example.bob.au/");
    assertRestricted(doc, "http://chrstian+1@cluebeck.bob.au/");

    // This is not working. Discussed with Alex and his opinion is that it shouldn't
    // be considered valid anyway (the short summary version anyway)
    //assertRestricted(doc, "/losch.bob.au/favicon.ico");
  }

  @Test
  public void testRuleDiffs() {
    // Nothing should be different yet
    RulesDiff diff = ruleSet.checkForRulesChanges(ruleSetSecondCopy);
    assertFalse("No rules should need work", diff.hasWorkLeft());

    RulesDiff.RulesWrapper changeRule = null;
    try {
      changeRule = diff.nextRule();
      fail("Should have thrown a NoSuchElementException");
    } catch (NoSuchElementException ex) {
      assertNull("Rule should still be null", changeRule);
    }

    // New we are going to change something
    CdxRule rule = ruleSetSecondCopy.getRules().get(1144L);
    rule.setPeriod(rule.getPeriod().minusYears(1));
    // And re-test
    diff = ruleSet.checkForRulesChanges(ruleSetSecondCopy);
    assertTrue("Rules should need work", diff.hasWorkLeft());
  }

  private void setUrl(Document doc, String url) {
    doc.setUrl(url);
    doc.setDeliveryUrl(canonUrl(url));
  }

  private String canonUrl(String input) {
    ParsedUrl url = ParsedUrl.parseUrl(input);
    Canonicalizer.AGGRESSIVE.canonicalize(url);
    return url.toString();
  }

  private void assertAccepted(Document doc, String url) throws CdxRestrictionService.RulesOutOfDateException {
    assertBlockRule(doc, url, DocumentStatus.ACCEPTED);
  }
  private void assertRestricted(Document doc, String url) throws CdxRestrictionService.RulesOutOfDateException {
    assertBlockRule(doc, url, DocumentStatus.RESTRICTED_FOR_BOTH);
  }
  private void assertBlockRule(Document doc, String url, DocumentStatus status) throws CdxRestrictionService.RulesOutOfDateException {
    setUrl(doc, url);
    assertEquals(status, service.filterDocument(doc).getIndexerPolicy());
  }
}
