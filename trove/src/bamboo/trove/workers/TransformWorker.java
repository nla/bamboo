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
import bamboo.trove.common.FilenameFinder;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.SearchCategory;
import bamboo.trove.common.SolrEnum;
import bamboo.util.Urls;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static bamboo.trove.services.QualityControlService.*;

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

  private boolean loop() {
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

    basicMetadata(solr, document);
    restrictions(solr, document);
    errorHandling(solr, document);
    fullText(solr, document);

    document.converted(solr);
  }

  private void basicMetadata(SolrInputDocument solr, IndexerDocument document) {
    solr.addField(SolrEnum.ID.toString(), document.getDocId());
    // Remove the protocol for Solr. Search clients get fuzzy matches
    String url = document.getBambooDocument().getUrl();
    String strippedUrl = Urls.removeScheme(url);
    // But we need to store the protocol (if there was one) to render an accurate delivery URL.
    if (!url.equals(strippedUrl)) {
      solr.addField(SolrEnum.PROTOCOL.toString(), url.substring(0, url.indexOf(":")));
    }
    solr.addField(SolrEnum.URL.toString(), strippedUrl);
    String filename = FilenameFinder.getFilename(document.getBambooDocument().getUrl());
    if (filename != null) {
      solr.addField(SolrEnum.FILENAME.toString(), filename);
    }
    solr.addField(SolrEnum.DATE.toString(), document.getBambooDocument().getDate());
    solr.addField(SolrEnum.TITLE.toString(), document.getBambooDocument().getTitle());
    solr.addField(SolrEnum.CONTENT_TYPE.toString(), document.getBambooDocument().getContentType());
    solr.addField(SolrEnum.STATUS_CODE.toString(), document.getBambooDocument().getStatusCode());
    solr.addField(SolrEnum.SITE.toString(), document.getBambooDocument().getSite());
    // We reverse the hostname (which is site + sub-domain) for efficient sub-domain wildcarding in Solr
    solr.addField(SolrEnum.HOST_REVERSED.toString(),
            (new StringBuffer(document.getBambooDocument().getHost())).reverse().toString());
    // If it is an AU gov website we index this
    if (document.getBambooDocument().getSite().endsWith(".gov.au")) {
      solr.addField(SolrEnum.AU_GOV.toString(), true);
    }

    // Optional metadata we _might_ get from html
    String description = document.getBambooDocument().getDescription();
    if (description != null && !"".equals(description)) {
      solr.addField(SolrEnum.DESCRIPTION.toString(), description);
    }
  }

  private void restrictions(SolrInputDocument solr, IndexerDocument document) {
    solr.addField(SolrEnum.RULE.toString(), document.getRuleId());
    // Don't populate if false
    //if (DocumentStatus.RESTRICTED_FOR_BOTH.equals(document.getStatus())) {
      //solr.addField(SolrEnum.DELIVERABLE.toString(), false);
      //solr.addField(SolrEnum.DISCOVERABLE.toString(), false);
    //}
    if (DocumentStatus.RESTRICTED_FOR_DELIVERY.equals(document.getStatus())) {
      //solr.addField(SolrEnum.DELIVERABLE.toString(), false);
      solr.addField(SolrEnum.DISCOVERABLE.toString(), true);
    }
    if (DocumentStatus.RESTRICTED_FOR_DISCOVERY.equals(document.getStatus())) {
      solr.addField(SolrEnum.DELIVERABLE.toString(), true);
      //solr.addField(SolrEnum.DISCOVERABLE.toString(), false);
    }
  }

  private void errorHandling(SolrInputDocument solr, IndexerDocument document) {
    if (document.getBambooDocument().getTextError() != null) {
      solr.addField(SolrEnum.TEXT_ERROR.toString(), true);
    }
  }

  private void fullText(SolrInputDocument solr, IndexerDocument document) {
    if (ContentThreshold.METADATA_ONLY.equals(document.getTheshold())) {
      solr.addField(SolrEnum.SEARCH_CATEGORY.toString(), SearchCategory.NONE.toString());
      return;
    }

    searchCategory(solr, document);

    //if (ContentThreshold.DOCUMENT_START_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == First X words
    //}
    //if (ContentThreshold.UNIQUE_TERMS_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == Only unique terms
    //}
    if (ContentThreshold.FULL_TEXT.equals(document.getTheshold())) {
      String text = document.getBambooDocument().getText();
      if (text == null) return;
      text = text.trim();

      if (!"".equals(text)) {
        solr.addField(SolrEnum.FULL_TEXT.toString(), text);
      }
    }
  }

  private void searchCategory(SolrInputDocument solr, IndexerDocument document) {
    SearchCategory category = SearchCategory.NONE;
    String type = document.getBambooDocument().getContentType();

    if (DOCUMENT_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.DOCUMENT;
    }
    if (HTML_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.HTML;
    }
    if (PDF_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.PDF;
    }
    if (PRESENTATION_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.PRESENTATION;
    }
    if (SPREADSHEET_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.SPREADSHEET;
    }

    solr.addField(SolrEnum.SEARCH_CATEGORY.toString(), category.toString());
  }
}
