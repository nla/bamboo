package bamboo.trove.common;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class exists solely for ferrying data between the App server and the dashboard. We don't won't to be
 * serializing all of a Warc's data every time the UI ajax tick fires so this minimal summary object carries
 * only what is required. The heaviest work required is when errors exist... but we can accept that edge case.
 */
public class WarcSummary {
  @JsonIgnore
  private WarcProgressManager warc;

  public WarcSummary(WarcProgressManager warc) {
    this.warc = warc;
  }

  @JsonProperty("warcId")
  public long getWarcId() {
    return warc.getWarcId();
  }

  @JsonProperty("timeStarted")
  public long getTimeStarted() {
    return warc.getTimeStarted();
  }

  @JsonProperty("loadComplete")
  public boolean isLoadComplete() {
    return warc.isLoadingComplete();
  }

  @JsonProperty("loadFailed")
  public boolean isLoadFailed() {
    return warc.isLoadingFailed();
  }

  @JsonProperty("documentCount")
  public int getDocumentCount() {
    return warc.size();
  }

  @JsonProperty("warcBytes")
  public long getWarcBytes() {
    return warc.getBatchBytes();
  }

  @JsonProperty("countFilter")
  public int getCountFilter() {
    return warc.getCountFilterCompleted();
  }

  @JsonProperty("countTransform")
  public int getCountTransform() {
    return warc.getCountTransformCompleted();
  }

  @JsonProperty("countIndex")
  public int getCountIndex() {
    return warc.getCountIndexCompleted();
  }

  @JsonProperty("hasError")
  public boolean getHasError() {
    return warc.hasErrors();
  }

  private static final List<String> noErrors = new ArrayList<>(0);
  @JsonProperty("getErrors")
  public List<String> getErrors() {
    if (warc.errorQ.isEmpty()) {
      return noErrors;
    }

    List<String> errors = new ArrayList<>();
    for (IndexerDocument doc : warc.errorQ) {
      String errorMessage = "Doc '" + doc.getDocId() + "' in error: ";
      if (doc.getFilterError() != null) {
        errorMessage += "Filter error: " + doc.getFilterError().getMessage();
      }
      if (doc.getTransformError() != null) {
        errorMessage += "Transform error: " + doc.getTransformError().getMessage();
      }
      if (doc.getIndexError() != null) {
        errorMessage += "Filter error: " + doc.getIndexError().getMessage();
      }
      errors.add(errorMessage);
    }
    return errors;
  }

  @JsonProperty("discardedErrors")
  public int getDiscardedErrors() {
    return warc.discardedErrors;
  }
}