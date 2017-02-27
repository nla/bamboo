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
package bamboo.trove.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * This class exists solely for ferrying data between the App server and the dashboard. We don't won't to be
 * serializing all of a Warc's data every time the UI ajax tick fires so this minimal summary object carries
 * only what is required. The heaviest work required is when errors exist... but we can accept that edge case.
 */
@SuppressWarnings("unused")
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

  @JsonProperty("documentEstimate")
  public long getDocumentEstimate() {
    return warc.getUrlCountEstimate();
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

  @JsonProperty("errorTracking")
  public Pair<Long, Integer> getErrorTracking() {
    if (warc.getErrorTracking() == null) {
      return null;
    }
    return new Pair<>(warc.getErrorTracking().getFirst().getTime(), warc.getErrorTracking().getSecond());
  }
}
