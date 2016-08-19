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
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.services.FilteringCoordinationService;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterWorker implements Runnable {
  private static Logger log = LoggerFactory.getLogger(FilterWorker.class);

  private FilteringCoordinationService filteringService;
  private Timer timer;
  private IndexerDocument lastJob = null;
  private IndexerDocument thisJob = null;

  public FilterWorker(FilteringCoordinationService filteringService, Timer timer) {
    this.filteringService = filteringService;
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
      log.error("Unexpected error during FilterWorker execution: ", ex);
      if (thisJob != null) {
        thisJob.setTransformError(ex);
      }
    }
    return true;
  }

  private void findJob() throws InterruptedException {
    thisJob = BaseWarcDomainManager.getNextFilterJob(lastJob);
    lastJob = null;
    while (thisJob == null) {
      Thread.sleep(1000);
      thisJob = BaseWarcDomainManager.getNextFilterJob(null);
    }
  }

  private void doJob() {
    thisJob.filter.start(timer);
    filteringService.filterDocument(thisJob);
    thisJob.filter.finish();
  }
}