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

import au.gov.nla.trove.indexer.api.AcknowledgableSolrInputDocument;
import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This class exists purely for diagnostic purposes. When testing performance improvements with multiple options for
 * talking to Solr, this class was added as a way of hotswapping in new end points without restarting. The indexer UI
 * was similarly altered to graph each endpoint independently.
 *
 * NOTE: This was not strongly considered in terms of thread safety for production use as it is a diagnostic class.
 */
public class EndPointRotator {
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(EndPointRotator.class);

  private static final int MINS = 5;
  private static final long ROTATION_TIMEOUT_MS = 1000 * 60 * MINS;

  static {
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        tick();
      }
    }, 0, ROTATION_TIMEOUT_MS);
  }

  private static final List<EndPointDomainManager> endPoints = new ArrayList<>();
  private static int index = 0;

  private static void tick() {
    if ((index + 1) >= endPoints.size()) {
      index = 0;
    } else {
      index++;
    }
  }

  public static void registerNewEndPoint(EndPointDomainManager manager) {
    endPoints.add(manager);
  }

  public static void add(SolrInputDocument doc, AcknowledgeWorker worker) {
    // Only make one call to get() in case the index changes whilst we are inside this method
    EndPointDomainManager endPoint = endPoints.get(index);
    if (endPoint.supportsAsyncAdd()) {
      endPoint.addAsync(new AsyncDocWrapper(doc, worker));
    } else {
      endPoint.add(doc, worker);
    }
  }

  private static class AsyncDocWrapper implements AcknowledgableSolrInputDocument {
    private SolrInputDocument document;
    private AcknowledgeWorker acknowledgeWorker;

    AsyncDocWrapper(SolrInputDocument document, AcknowledgeWorker acknowledgeWorker) {
      this.document = document;
      this.acknowledgeWorker = acknowledgeWorker;
    }

    @Override
    public SolrInputDocument getSolrDocument() {
      return document;
    }

    @Override
    public void acknowledge(SolrInputDocument solrInputDocument) {
      acknowledgeWorker.acknowledge(solrInputDocument);
    }

    @Override
    public void errorProcessing(SolrInputDocument solrInputDocument, Throwable throwable) {
      acknowledgeWorker.errorProcessing(solrInputDocument, throwable);
    }
  }
}
