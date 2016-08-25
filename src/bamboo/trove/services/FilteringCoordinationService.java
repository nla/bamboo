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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;

import bamboo.task.Document;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.IndexerDocument;
import bamboo.util.SurtFilter;
import bamboo.util.Urls;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.UniformReservoir;
import org.archive.url.SURT;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

@Service
public class FilteringCoordinationService {
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
  private String[] excludedCharacters = {"\\", "/", ",", ".", ":", ";", "?", "\""};
  private String cleanKeyPattern = "";
  @PostConstruct
  public void init() {
    cleanKeyPattern += "[";
    for (String c : excludedCharacters) {
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
    DocumentStatus status = DocumentStatus.REJECTED;
    if (!threshold.equals(ContentThreshold.NONE)) {
      status = bambooRestrictionService.filterDocument(document.getBambooDocument());
    }
    document.applyFiltering(status, threshold);

    if (collectMetrics) {
      collectMetrics(document);
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

  private void collectMetrics(IndexerDocument document) {
    lazyLoadChecks(document);
    recordSizeByKey(thresholdSizes, document.getTheshold(), document);
    // Cutout here if we aren't going to index it
    if (document.getTheshold().equals(ContentThreshold.NONE)) return;

    recordSizeByDomain(document);
    recordSizeByKey(statusSizes,    document.getStatus(),   document);
    recordSizeByKey(typeSizes,      cleanType(document), document);
    recordSizeByKey(codeSizes, "" + document.getBambooDocument().getStatusCode(), document);
    recordYear(document.getBambooDocument());
  }

  private void recordYear(Document doc) {
    // Avoid going through sdf twice and do both the lazy load and work in one step
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
    String year = sdf.format(doc.getDate());
    if (!yearSizes.containsKey(year)) {
      Histogram h = new Histogram(new UniformReservoir());
      yearSizes.put(year, h);
      metrics.register("size.year." + year, h);
    }
    yearSizes.get(year).update(doc.getContentLength());
  }

  private void recordSizeByDomain(IndexerDocument document) {
    Document doc = document.getBambooDocument();
    String surt = SURT.toSURT(Urls.removeScheme(doc.getUrl()));
    domainSizes.keySet().stream()
            .filter(thisFilter -> thisFilter.accepts(surt))
            .forEach(thisFilter -> domainSizes.get(thisFilter).update(doc.getContentLength()));
  }
  private void recordSizeByKey(Map<?, Histogram> map, Object key, IndexerDocument document) {
    map.get(key).update(document.getBambooDocument().getContentLength());
  }

  private void lazyLoadChecks(IndexerDocument document) {
    lazyLoadByKey(statusSizes,    document.getStatus(),   "size.status.");
    lazyLoadByKey(thresholdSizes, document.getTheshold(), "size.threshold.");
    lazyLoadByKey(typeSizes,      cleanType(document),    "size.type.");
    lazyLoadByKey(codeSizes, "" + document.getBambooDocument().getStatusCode(), "size.code.");
  }

  private void lazyLoadByKey(Map map, Object key, String prefix) {
    if (!map.containsKey(key)) {
      synchronized (map) {
        if (!map.containsKey(key)) {
          Histogram h = new Histogram(new UniformReservoir());
          map.put(key, h);
          metrics.register(prefix + key.toString() + "", h);
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
    return document.getBambooDocument().getContentType().replaceAll(cleanKeyPattern, "");
  }
}