/**
 * Copyright 2016 National Library of Australia
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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;

import bamboo.task.Document;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.Rule;
import bamboo.util.SurtFilter;
import bamboo.util.Urls;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.UniformReservoir;
import org.archive.url.SURT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

@Service
public class FilteringCoordinationService {
  private static Logger log = LoggerFactory.getLogger(FilteringCoordinationService.class);
  private boolean collectMetrics = true;

  @Autowired
  private BambooRestrictionService bambooRestrictionService;

  @Autowired
  private QualityControlService qualityControlService;

  private String metricsRegistryName;
  private MetricRegistry metrics;

  @Required
  public void setMetricsRegistryName(String metricsRegistryName) {
    this.metricsRegistryName = metricsRegistryName;
  }

  public String getMetricsRegistryName() {
    return metricsRegistryName;
  }

  // Things we don't want to see in stat keys when lazy loading dirty data
  private static final String[] EXCLUDED_CHARACTERS = {"\\", "/", ",", ".", ":", ";", "?", "\""};
  private static final List<String> ALLOWED_TYPES = Arrays.asList(
          "texthtml", "imagejpeg", "imagegif", "applicationpdf", "imagepng", "textplain", "textcss", "textxml");
  private String cleanKeyPattern = "";

  @PostConstruct
  public void init() {
    cleanKeyPattern += "[";
    for (String c : EXCLUDED_CHARACTERS) {
      cleanKeyPattern += "\\" + c;
    }
    cleanKeyPattern += "]*";
    metrics = SharedMetricRegistries.getOrCreate(metricsRegistryName);
    loadDomainMetrics();
  }

  public void setCollectMetrics(boolean collectMetrics) {
    this.collectMetrics = collectMetrics;
  }

  public void filterDocument(IndexerDocument document) {
    ContentThreshold threshold = qualityControlService.filterDocument(document.getBambooDocument());
    DocumentStatus status = DocumentStatus.NOT_APPLICABLE;
    Rule rule = null;
    if (!threshold.equals(ContentThreshold.NONE)) {
    	//TODO document needs to hold a list of dates
    	// so that date rule can be applied to each date and the record split if needed
      Map<Rule, List<Date>> rules = bambooRestrictionService.filterDocument(document.getBambooDocument());
      rule = rules.keySet().iterator().next();
      status = rule.getPolicy();
    }
    document.applyFiltering(rule, threshold);

    if (collectMetrics) {
      // Never fail because of this secondary stuff
      try {
        collectMetrics(document, status);

      } catch (Exception ex) {
        log.error("Metrics failed on document: {}", document.getDocId(), ex);
      }
    }
  }

  //********************************
  // Everything below here is related purely to metrics on content
  private final Map<DocumentStatus, Histogram> statusSizes = new HashMap<>();
  private final Map<ContentThreshold, Histogram> thresholdSizes = new HashMap<>();
  private final Map<String, Histogram> typeSizes = new HashMap<>();
  private final Map<String, Histogram> codeSizes = new HashMap<>();
  private final Map<SurtFilter, Histogram> domainSizes = new HashMap<>();
  private final Map<String, Histogram> yearSizes = new HashMap<>();

  private long contentLength(Document document) {
    // At first we were capturing the content length as measured by Bamboo,
    // but that is less useful to us when collecting analytics for the indexer 
    //return document.getContentLength();
    if (document.getText() == null) {
      return 0;
    }
    return document.getText().length();
  }

  private void collectMetrics(IndexerDocument document, DocumentStatus status) {
    lazyLoadChecks(document, status);
    recordSizeByKey(thresholdSizes, document.getTheshold(), document);
    // Cutout here if we aren't going to index it
    //if (document.getTheshold().equals(ContentThreshold.NONE)) return;

    recordSizeByDomain(document);
    recordSizeByKey(statusSizes,    status,   document);
    recordSizeByKey(typeSizes,      cleanType(document), document);
    recordSizeByKey(codeSizes, "" + document.getBambooDocument().getStatusCode(), document);
    recordYear(document.getBambooDocument());
  }

  private void recordYear(Document doc) {
    // Avoid going through sdf twice and do both the lazy load and work in one step
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
    String year = sdf.format(doc.getDate());
    yearHistogram(year).update(contentLength(doc));
  }

  private Histogram yearHistogram(String year) {
    if (!yearSizes.containsKey(year)) {
      synchronized (yearSizes) {
        if (!yearSizes.containsKey(year)) {
          Histogram h = new Histogram(new UniformReservoir());
          metrics.register("size.year." + year, h);
          yearSizes.put(year, h);
        }
      }
    }
    return yearSizes.get(year);
  }

  private void recordSizeByDomain(IndexerDocument document) {
    Document doc = document.getBambooDocument();
    String surt = SURT.toSURT(Urls.removeScheme(doc.getUrl()));
    domainSizes.keySet().stream()
            .filter(thisFilter -> thisFilter.accepts(surt))
            .forEach(thisFilter -> domainSizes.get(thisFilter).update(contentLength(doc)));
  }
  private void recordSizeByKey(Map<?, Histogram> map, Object key, IndexerDocument document) {
    if (key == null) return;
    map.get(key).update(contentLength(document.getBambooDocument()));
  }

  private void lazyLoadChecks(IndexerDocument document, DocumentStatus status) {
    lazyLoadByKey(statusSizes,    status,   "size.status.");
    lazyLoadByKey(thresholdSizes, document.getTheshold(), "size.threshold.");
    lazyLoadByKey(typeSizes,      cleanType(document),    "size.type.");
    lazyLoadByKey(codeSizes, "" + document.getBambooDocument().getStatusCode(), "size.code.");
  }

  private void lazyLoadByKey(Map map, Object key, String prefix) {
    if (key == null) return;
    if (!map.containsKey(key)) {
      synchronized (map) {
        if (!map.containsKey(key)) {
          Histogram h = new Histogram(new UniformReservoir());
          metrics.register(prefix + key.toString().replaceAll("\\*", "star") + "", h);
          map.put(key, h);
        }
      }
    }
  }

  private void loadDomainMetrics() {
    // XYZ + XYZ.au
    loadTopLevelDomainFilter("gov");
    loadTopLevelDomainFilter("edu");
    loadTopLevelDomainFilter("com");
    loadTopLevelDomainFilter("org");
    loadTopLevelDomainFilter("net");
    // Other than above
    newDomainFilterHistogram("+\n-(gov\n-(au,gov\n" + "-(edu\n-(au,edu\n"
            + "-(com\n-(au,com\n" + "-(org\n-(au,org\n" + "-(net\n-(au,net\n", "size.domain.other");

    // Overlapping by state (XYZ.gov.au + XYZ.edu.au)
    loadStateDomainFilter("act");
    loadStateDomainFilter("nsw");
    loadStateDomainFilter("nt");
    loadStateDomainFilter("qld");
    loadStateDomainFilter("sa");
    loadStateDomainFilter("tas");
    loadStateDomainFilter("vic");
    loadStateDomainFilter("wa");
  }
  private void loadTopLevelDomainFilter(String domain) {
    newDomainFilterHistogram("-\n+(au," + domain, "size.domain.au." + domain);
    newDomainFilterHistogram("-\n+(" + domain, "size.domain." + domain);
  }
  private void loadStateDomainFilter(String state) {
    newDomainFilterHistogram("-\n+(au,gov," + state, "size.domain.state.gov." + state);
    newDomainFilterHistogram("-\n+(au,edu," + state, "size.domain.state.edu." + state);
  }

  private void newDomainFilterHistogram(String rules, String metricsName) {
    Histogram h = new Histogram(new UniformReservoir());
    domainSizes.put(new SurtFilter(rules), h);
    metrics.register(metricsName, h);
  }

  private String cleanType(IndexerDocument document) {
    String type = document.getBambooDocument().getContentType().replaceAll(cleanKeyPattern, "");
    if (ALLOWED_TYPES.contains(type)) {
      return type;
    }
    return "other";
  }
}