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
package bamboo.trove.workers;

import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.SolrEnum;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformWorker implements Runnable {
  private static Logger log = LoggerFactory.getLogger(TransformWorker.class);

  private Timer timer;
  private IndexerDocument lastJob = null;
  private IndexerDocument thisJob = null;

  public TransformWorker(Timer timer) {
    this.timer = timer;
  }

  @Override
  public void run() {
    while (loop()) {}
  }

  public boolean loop() {
    try {
      findJob();
      doJob();
      lastJob = thisJob;
      thisJob = null;

    } catch (InterruptedException ex) {
      log.error("Thread.Sleep() interrupted on worker that cannot find job. Resuming. MSG:'{}'", ex.getMessage());
      return true;

    } catch (Exception ex) {
      log.error("Unexpected error during TransformWorker execution: ", ex);
      if (thisJob != null) {
        thisJob.setTransformError(ex);
      }
      lastJob = thisJob;
      thisJob = null;
    }
    return true;
  }

  private void findJob() throws InterruptedException {
    thisJob = BaseWarcDomainManager.getNextTransformJob(lastJob);
    lastJob = null;
    while (thisJob == null) {
      Thread.sleep(1000);
      thisJob = BaseWarcDomainManager.getNextTransformJob(null);
    }
  }

  private void doJob() {
    thisJob.transform.start(timer);
    transform(thisJob);
    thisJob.transform.finish();
  }

  private void transform(IndexerDocument document) {
    // No indexing at all
    if (ContentThreshold.NONE.equals(document.getTheshold())) {
      return;
    }

    SolrInputDocument solr = new SolrInputDocument();
    solr.addField(SolrEnum.ID.toString(), document.getDocId());
    solr.addField(SolrEnum.URL.toString(), document.getBambooDocument().getUrl());
    solr.addField(SolrEnum.DATE.toString(), document.getBambooDocument().getDate());
    solr.addField(SolrEnum.TITLE.toString(), document.getBambooDocument().getTitle());
    solr.addField(SolrEnum.CONTENT_TYPE.toString(), document.getBambooDocument().getContentType());
    solr.addField(SolrEnum.SITE.toString(), document.getBambooDocument().getSite());
    if (DocumentStatus.RESTRICTED_FOR_BOTH.equals(document.getStatus())) {
      solr.addField(SolrEnum.DELIVERABLE.toString(), false);
      solr.addField(SolrEnum.DISCOVERABLE.toString(), false);
    }
    if (DocumentStatus.RESTRICTED_FOR_DELIVERY.equals(document.getStatus())) {
      solr.addField(SolrEnum.DELIVERABLE.toString(), false);
      solr.addField(SolrEnum.DISCOVERABLE.toString(), true);
    }
    if (DocumentStatus.RESTRICTED_FOR_DISCOVERY.equals(document.getStatus())) {
      solr.addField(SolrEnum.DELIVERABLE.toString(), true);
      solr.addField(SolrEnum.DISCOVERABLE.toString(), false);
    }

    if (document.getBambooDocument().getTextError() != null) {
      solr.addField(SolrEnum.TEXT_ERROR.toString(), true);
    }

    if (ContentThreshold.METADATA_ONLY.equals(document.getTheshold())) {
      document.converted(solr);
      return;
    }

    if (ContentThreshold.DOCUMENT_START_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == First X words
    }
    if (ContentThreshold.UNIQUE_TERMS_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == Only unique terms
    }
    if (ContentThreshold.FULL_TEXT.equals(document.getTheshold())) {
      // TODO: Full text == Everything
    }

    document.converted(solr);
  }
}