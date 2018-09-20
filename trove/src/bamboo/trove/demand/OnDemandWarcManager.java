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
package bamboo.trove.demand;

import bamboo.task.WarcToIndex;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.ToIndex;
import bamboo.trove.common.WarcProgressManager;
import bamboo.trove.rule.RuleChangeUpdateManager;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class OnDemandWarcManager extends BaseWarcDomainManager {
  private static final Logger log = LoggerFactory.getLogger(OnDemandWarcManager.class);

  // We don't really need this, but we want Spring to start it before us, so we list it as a dependency
  @SuppressWarnings("unused")
	@Autowired(required = true)
	private RuleChangeUpdateManager ruleChangeUpdateManager;

  private boolean running = false;
  private boolean starting = false;
  private long warcsProcessed = 0;
  private long lastWarcId = 0;

  private Integer outstandingBatches = 0;

  @PostConstruct
  public void init() throws InterruptedException {
    waitUntilStarted();
		log.info("***** OnDemandWarcManager *****");
		log.info("Run at start        : {}", runAtStart);
  }

  // The UI will call here when it wants to start indexing a warc
	public String index(long warcId) throws Exception {
    return index(warcId, -1);
  }
  // Same as above, but flagging a particular offset as being of interest
	public String index(long warcId, long warcOffset) throws Exception {
    if (!running) {
      return "<error>Offline</error>";
    }
    synchronized (this) {
      outstandingBatches++;
    }

    log.info("Indexing on demand. Warc #{}", warcId);
    // Fake up the objects we normally build in communication with Bamboo
    WarcToIndex warcToIndex = new WarcToIndex(warcId, 0);
    ToIndex toIndex = new ToIndex(warcToIndex);
    toIndex.setTrackedOffset(warcOffset);

    WarcProgressManager batch = getAndEnqueueWarc(toIndex);
    IndexerDocument responseDocument = batch.getTrackedDocument();
    if(responseDocument != null){
      responseDocument.setHoldSolrDocument(true);
    }
    log.info("Warc #{} has {} documents. Loading has completed.", warcId, batch.size());

    // TODO: A fair bit more thinking needs to go into error handling here. The complexity was increased dramatically
    // here when the Full Corpus domain was added, but this code remains pretty basic. Now we need to know the definite
    // state of the batch in terms of managing the outstandingBatches counter to be sure that nothing is indexing
    // when the restiction service runs nightly.

    while (!batch.isFilterComplete()) {
      Thread.sleep(100);
      checkErrors(batch);
    }
    log.info("Warc #{} has finished filtering...", warcId);

    while (!batch.isTransformComplete()) {
      Thread.sleep(100);
      checkErrors(batch);
    }
    log.info("Warc #{} has finished transform...", warcId);

    while (!batch.isIndexComplete()) {
      Thread.sleep(100);
      checkErrors(batch);
    }
    log.info("Warc #{} has finished indexing...", warcId);

    warcsProcessed++;
    synchronized (this) {
      outstandingBatches--;
    }
    lastWarcId = warcId;

    if(responseDocument == null){
      return "<ResultNotFound/>";
    }
    return ClientUtils.toXML(responseDocument.getSolrDocument());
  }

  private void checkErrors(WarcProgressManager warc) throws Exception {
    if (warc.hasErrors()) {
      log.error("Warc #{} failed to index.", warc.getWarcId());
      throw new Exception("Indexing failed");
    }
  }

  public void run() {
    if (!running && !starting)  {
      acquireDomainStartLock();
      try {
        if (!running && !starting)  {
          // TODO... is there anything more complicated to do here?
          log.info("Starting...");
          running = true;
        }
      } finally {
        releaseDomainStartLock();
      }
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isStopping() {
    return false;
  }

  @Override
  public void start() {
  	if(BaseWarcDomainManager.isDisableIndexing()){
  		throw new IllegalStateException("Cannot start because indexing is disabled.");
  	}
    // Spawn a new thread to start/restart the domain. The restrictions domain should/may be holding the lock so it won't
    // do anything yet, but we do it in another thread to allow this thread to return after the stop() call.
    Thread thread = new Thread(this);
    thread.setName(getName());
    thread.start();
  }

  @Override
  public void stop() {
    if (running)  {
      log.info("Stopping domain");
      while (running) {
        // Check if all of the outstanding work is finished
        synchronized (this) {
          if (outstandingBatches > 0) {
            log.info("There are {} batch(es) still processing. Waiting...", outstandingBatches);
          } else {
            running = false;
          }
        }

        // If we are still running, sleep for a bit and try again
        if (running) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            log.error("Thread sleep interrupted. Resuming. Reason: ", e.getMessage());
          }
        }
      }
    }
  }

  @Override
  public String getName() {
    return "Web Archives On-Demand Indexing";
  }

  @Override
  public long getUpdateCount() {
    return warcsProcessed;
  }

  @Override
  public String getLastIdProcessed() {
    return "warc#" + lastWarcId;
  }
}
