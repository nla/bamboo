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

import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import bamboo.task.Document;
import bamboo.trove.common.cdx.CdxRule;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerDocument implements AcknowledgeWorker {
  @SuppressWarnings("unused")
  private static Logger log = LoggerFactory.getLogger(IndexerDocument.class);

  private String docId;
  public String getDocId() {
    return docId;
  }

  private float boost = 1.0f;
  public float getBoost() {
    return boost;
  }
  public float modifyBoost(float modifier) {
    boost *= modifier;
    return boost;
  }

  //***********************************
  // Step 1) Get from Bamboo
  private Document bambooDocument;
  public IndexerDocument(long warcId, Document bambooDocument) {
    if (bambooDocument == null) {
      throw new IllegalArgumentException("Null document provided");
    }
    if (bambooDocument.getWarcOffset() < 0) {
      throw new IllegalArgumentException("Invalid warc offset provided in document");
    }
    this.bambooDocument = bambooDocument;
    docId = "" + warcId + "/" + bambooDocument.getWarcOffset();
  }
  public Document getBambooDocument() {
    return bambooDocument;
  }

  //***********************************
  // Step 2) Filtering
  private CdxRule rule = null;
  private ContentThreshold threshold = null;
  public StateTracker filter = new StateTracker("Filtering");
  public void applyFiltering(CdxRule rule, ContentThreshold threshold) {
    this.rule = rule;
    this.threshold = threshold;
  }
  public DocumentStatus getStatus() {
    if (rule == null) return null; 
    return rule.getIndexerPolicy();
  }
  public ContentThreshold getThreshold() {
    return threshold;
  }
  public long getRuleId(){
    return rule.getId();
  }
  private Throwable filterError = null;
  Throwable getFilterError() {
    return filterError;
  }
  public void setFilterError(Throwable filterError) {
    this.filterError = filterError;
    if (filter.hasStarted() && !filter.hasFinished()) {
      filter.finish();
    }
  }

  //***********************************
  // Step 3) Conversion work
  private SolrInputDocument solrDocument;
  public void converted(SolrInputDocument solrDocument) {
    this.solrDocument = solrDocument;
  }
  public SolrInputDocument getSolrDocument() {
    return solrDocument;
  }
  public StateTracker transform = new StateTracker("Work");
  private Throwable transformError = null;
  Throwable getTransformError() {
    return transformError;
  }
  public void setTransformError(Throwable transformError) {
    this.transformError = transformError;
    if (transform.hasStarted() && !transform.hasFinished()) {
      transform.finish();
    }
  }

  //***********************************
  // Step 4) Write to Solr
  public StateTracker index = new StateTracker("Writing");
  private Throwable indexError = null;
  Throwable getIndexError() {
    return indexError;
  }
  public void setIndexError(Throwable indexError) {
    this.indexError = indexError;
    if (index.hasStarted() && !index.hasFinished()) {
      index.finish();
    }
  }

  // After Solr is finished the indexer will call one of these methods.
  @Override
  public void acknowledge(SolrInputDocument solrInputDocument) {
    index.finish();
  }
  @Override
  public void errorProcessing(SolrInputDocument solrInputDocument, Throwable throwable) {
    this.setIndexError(throwable);
  }

  boolean isInError() {
    return (filterError != null || transformError != null || indexError != null);
  }

  public class StateTracker {
    private String name;
    private boolean started = false;
    private boolean finished = false;
    private Timer.Context timer = null;

    StateTracker(String name) {
      this.name = name;
    }
    public void start(Timer timer) {
      if (started) {
        throw new IllegalStateException(name
                + " has already started! Looks like a threading bug trying to process the same object twice.");
      }
      if (timer == null) {
        throw new IllegalArgumentException("Null parameter provided.");
      }
      started = true;
      this.timer = timer.time();
    }
    public synchronized void finish() {
      if (!started) {
        throw new IllegalStateException("Cannot stop before starting.");
      }
      if (finished) {
        throw new IllegalStateException(name
                + " has already stopped! Looks like a threading bug trying to process the same object twice.");
      }
      finished = true;
      timer.stop();
    }
    boolean hasStarted() {
      return started;
    }
    boolean hasFinished() {
      return finished;
    }
  }
}
