package bamboo.trove.services;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;

import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.IndexerDocument;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.UniformReservoir;
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

  private void collectMetrics(IndexerDocument document) {
    lazyLoadChecks(document);

    recordSizeByKey(statusSizes,    document.getStatus(),   document);
    recordSizeByKey(thresholdSizes, document.getTheshold(), document);
    recordSizeByKey(typeSizes,      cleanType(document),    document);
    recordSizeByKey(codeSizes, "" + document.getBambooDocument().getStatusCode(), document);
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

  private String cleanType(IndexerDocument document) {
    return document.getBambooDocument().getContentType().replaceAll(cleanKeyPattern, "");
  }
}