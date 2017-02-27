/*
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
import bamboo.trove.common.EndPointRotator;
import bamboo.trove.common.IndexerDocument;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerWorker implements Runnable {
  private static Logger log = LoggerFactory.getLogger(IndexerWorker.class);

  private Timer timer;
  private Timer dqTimer;
  private IndexerDocument lastJob = null;
  private IndexerDocument thisJob = null;

  public IndexerWorker(Timer timer) {
    this.timer = timer;
    // TODO: Remove dqTimer. it has no long term usefulness. It is just being used during development
    // to keep an eye on thread backlogs (or the possibility thereof) around the solr.add() method at
    // various levels of scale.
    dqTimer = SharedMetricRegistries.getOrCreate("serverMetrics").timer("dqTimer");
    if (dqTimer == null) {
      dqTimer = new Timer();
      SharedMetricRegistries.getOrCreate("serverMetrics").register("dqTimer", dqTimer);
    }
  }

  @Override
  public void run() {
    while (loop()) {}
  }

  private boolean loop() {
    try {
      findJob();
      doJob();

    } catch (InterruptedException ex) {
      log.error("Thread.Sleep() interrupted on worker that cannot find job. Resuming. MSG:'{}'", ex.getMessage());
      return true;

    } catch (Exception ex) {
      log.error("Unexpected error during IndexerWorker execution: ", ex);
      if (thisJob != null) {
        thisJob.setIndexError(ex);
      }
    }
    lastJob = thisJob;
    thisJob = null;
    return true;
  }

  private void findJob() throws InterruptedException {
    thisJob = BaseWarcDomainManager.getNextIndexJob(lastJob);
    lastJob = null;
    while (thisJob == null) {
      Thread.sleep(1000);
      thisJob = BaseWarcDomainManager.getNextIndexJob(null);
    }
  }

  private void doJob() {
    Timer.Context ctx = dqTimer.time();
    try {
      thisJob.index.start(timer);
      if (thisJob.getTheshold() == null || thisJob.getSolrDocument() == null
              || thisJob.getTheshold().equals(ContentThreshold.NONE)) {
        thisJob.index.finish();
        return;
      }
      // IndexerDocument implements AcknowledgeWorker so it will handle the timer.stop() and/or errors
      EndPointRotator.add(thisJob.getSolrDocument(), thisJob);

    } finally {
      ctx.stop();
    }
  }
}
