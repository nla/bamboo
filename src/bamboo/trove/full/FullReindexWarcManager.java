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
package bamboo.trove.full;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.PostConstruct;

import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.WarcProgressManager;
import bamboo.trove.services.FilteringCoordinationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

@Service
public class FullReindexWarcManager extends BaseWarcDomainManager {
  private static final Logger log = LoggerFactory.getLogger(FullReindexWarcManager.class);
  private static final int POLL_INTERVAL_SECONDS = 1;

  private long warcMin = 127536;
  private long warcMax = 127773;

	@Autowired
	@Qualifier("solrDomainManager")
	private EndPointDomainManager solrManager;

	@Autowired
	private FilteringCoordinationService filteringService;

  private String bambooBaseUrl;
  private int maxFilterWorkers;
  private int maxTransformWorkers;
  private int maxIndexWorkers;

  private boolean running = false;
  private boolean stopping = false;
  private boolean finishedFinding = false;
  private long warcsProcessed = 0;
  private long lastWarcId = warcMin - 1;

  private WorkProcessor readPool;
  private List<ReadWorker> readWorkers = new ArrayList<>();
  private int bambooReadThreads = 1;
  private Queue<Long> warcIdQueue = new ConcurrentLinkedQueue<>();

  private Map<Long, WarcProgressManager> warcTracking = new TreeMap<>();
  private int queueLimit = 5;
  private Timer timer;

  public void setBambooReadThreads(int bambooReadThreads) {
    this.bambooReadThreads = bambooReadThreads;
  }

  public void setQueueLimit(int queueLimit) {
    this.queueLimit = queueLimit;
  }

  @Required
  public void setBambooBaseUrl(String bambooBaseUrl) {
    this.bambooBaseUrl = bambooBaseUrl;
  }

  @Required
  public void setMaxFilterWorkers(int maxFilterWorkers) {
    this.maxFilterWorkers = maxFilterWorkers;
  }

  @Required
  public void setMaxTransformWorkers(int maxTransformWorkers) {
    this.maxTransformWorkers = maxTransformWorkers;
  }

  @Required
  public void setMaxIndexWorkers(int maxIndexWorkers) {
    this.maxIndexWorkers = maxIndexWorkers;
  }

  @PostConstruct
  public void init() {
		log.info("***** FullReindexWarcManager *****");
    BaseWarcDomainManager.startMe(bambooBaseUrl, maxFilterWorkers, maxTransformWorkers, maxIndexWorkers,
            solrManager, filteringService);
    log.info("Warc read threads : {}", bambooReadThreads);
    log.info("Warc queue limit  : {}", queueLimit);
    log.info("Run at start      : {}", runAtStart);
    readPool = new WorkProcessor(bambooReadThreads);
    checkBatches();
  }

  private void startWorkers() {
    for (int i = 0; i < bambooReadThreads; i++) {
      ReadWorker worker = new ReadWorker();
      readWorkers.add(worker);
      readPool.process(worker);
    }
  }

  private void stopWorkers() {
    // Tell each worker to stop
    for (int i = 0; i < bambooReadThreads; i++) {
      readWorkers.get(0).stop();
      // Then dereference them from the domain
      readWorkers.remove(0);
    }
    // Wait until the thread confirms they all stopped
    readPool.stop();

    running = false;
    stopping = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isStopping() {
    return stopping;
  }

  @Override
  public void start() {
    if (!running && !stopping)  {
      log.info("Starting...");
      running = true;
      Thread me = new Thread(this);
      me.setName(getName());
      me.start();
    }
  }

  public void run() {
    startWorkers();
    loop();
  }

  @Override
  public void stop() {
    if (running && !stopping)  {
      if (timer != null) {
        timer.cancel();
      }
      stopping = true;
      stopWorkers();
    }
  }

  @Override
  public String getName() {
    return "Web Archives Full Corpus Indexing";
  }

  @Override
  public long getUpdateCount() {
    return warcsProcessed;
  }

  @Override
  public String getLastIdProcessed() {
    return "warc#" + lastWarcId;
  }


  private void loop() {
    while (running && !stopping&& !finishedFinding) {
      try {
        doWork();
      } catch (InterruptedException e) {
        log.error("Thread sleep interrupted whilst waiting on batch completion. Resuming: {}", e.getMessage());
      }
    }
  }

  private void doWork() throws InterruptedException {
    // Work complete
    if ((lastWarcId + 1) > warcMax) {
      finishedFinding = true;
      return;
    }
    // Saturated queue
    if ((warcTracking.size() + warcIdQueue.size()) >= queueLimit) {
      Thread.sleep(1000);
      return;
    }

    log.info("Enqueueing {}", lastWarcId + 1);
    warcIdQueue.offer(lastWarcId + 1);
    lastWarcId++;
  }

  private void setTick() {
    if (timer == null) {
      timer = new Timer();
    }
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        checkBatches();
      }
    }, POLL_INTERVAL_SECONDS * 1000);
  }

  private void checkBatches() {
    // Check state
    List<Long> completedWarcs = new ArrayList<>();
    for (Long key : warcTracking.keySet()) {
      WarcProgressManager warc = warcTracking.get(key);
      if (warc.finishedWithoutError()) {
        completedWarcs.add(key);
      } else {
        if (warc.finished()) {
          log.error("Warc {} is completed but has errors!", key);
        }
      }
    }

    // Cleanup
    for (Long warcId : completedWarcs) {
      log.info("De-referencing completed warc: {}", warcId);
      warcTracking.remove(warcId);
    }

    // Keep going unless we are done
    if (!(finishedFinding && warcTracking.isEmpty())) {
      setTick();
    } else {
      stop();
    }
  }

  private class ReadWorker implements Runnable {
    private boolean stop = false;

    @Override
    public void run() {
      log.info("Worker thread starting.");
      // TODO : full lifecycle needs more work. Stop/Start etc. This one just runs once
      while (!stop) {
        // Find work
        Long warcId = warcIdQueue.poll();
        if (warcId == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            log.error("Caught exception: ", e);
          }
          continue;
        }
        log.info("Seeking warc {}", warcId);
        // Do work
        WarcProgressManager batch = getAndEnqueueWarc(warcId);
        log.info("Warc #{} retrieval complete. {} docs", warcId, batch.size());
        // Track state
        warcTracking.put(warcId, batch);
      }
      log.info("Worker thread exiting.");
    }

    public void stop() {
      stop = true;
    }
  }
}