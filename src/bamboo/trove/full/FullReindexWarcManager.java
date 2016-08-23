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
import java.util.Date;
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
import bamboo.trove.common.WarcSummary;
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
  private Map<Long, WarcSummary> warcSummaries = new TreeMap<>();
  private int queueLimit = 5;
  private Timer timer;

  private Map<Long, Integer> noSpamErrors = new TreeMap<>();
  private Map<Long, Integer> noSpamTimeout = new TreeMap<>();

  public void setBambooReadThreads(int bambooReadThreads) {
    this.bambooReadThreads = bambooReadThreads;
  }

  public void setQueueLimit(int queueLimit) {
    this.queueLimit = queueLimit;
  }

  public Map<Long, WarcSummary> getBatchMap() {
    return warcSummaries;
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
      log.info("Work complete... I'm outta here");
      finishedFinding = true;
      return;
    }
    // Saturated queue
    if ((warcTracking.size() + warcIdQueue.size()) >= queueLimit) {
      Thread.sleep(1000);
      return;
    }

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
        if (warc.finished() || warc.isLoadingFailed()) {
          // First time
          if (!noSpamErrors.containsKey(key)) {
            log.error("Warc {} is completed but has errors!", key);
            noSpamErrors.put(key, 0);
          // Or every 5 minutes
          } else {
            int was = noSpamErrors.get(key);
            if (was > (300 / POLL_INTERVAL_SECONDS)) {
              log.error("Warc {} is completed but has errors!", key);
              noSpamErrors.put(key, 0);
            } else {
              noSpamErrors.put(key, was + 1);
            }
          }
        } else {
          if ((new Date().getTime() - warc.getTimeStarted()) > 300000) {
            // First time
            if (!noSpamTimeout.containsKey(key)) {
              log.warn("Warc {} has been pending for a long time. {} URLs, F#{}, T#{}, I#{}", key, warc.size(),
                      warc.getCountFilterCompleted(), warc.getCountTransformCompleted(), warc.getCountIndexCompleted());
              noSpamTimeout.put(key, 0);
            // Or every 5 minutes
            } else {
              int was = noSpamTimeout.get(key);
              if (was > (300 / POLL_INTERVAL_SECONDS)) {
                log.warn("Warc {} has been pending for a long time. {} URLs, F#{}, T#{}, I#{}", key, warc.size(),
                        warc.getCountFilterCompleted(), warc.getCountTransformCompleted(), warc.getCountIndexCompleted());
                noSpamTimeout.put(key, 0);
              } else {
                noSpamTimeout.put(key, was + 1);
              }
            }
          }
        }
      }
    }

    // Cleanup
    for (Long warcId : completedWarcs) {
      log.info("De-referencing completed warc: {}", warcId);
      warcTracking.remove(warcId);
      warcSummaries.remove(warcId);
    }

    // Keep going unless we are done
    if (!(finishedFinding && warcTracking.isEmpty())) {
      setTick();
    } else {
      stop();
    }
  }

  @Override
  protected WarcProgressManager newWarc(long warcId, long trackedOffset) {
    WarcProgressManager newWarc = new WarcProgressManager(warcId, trackedOffset);
    warcTracking.put(warcId, newWarc);
    warcSummaries.put(warcId, new WarcSummary(newWarc));
    return newWarc;
  }

  private class ReadWorker implements Runnable {
    private boolean stop = false;

    @Override
    public void run() {
      // TODO : full lifecycle needs more work. Stop/Start etc. This one just runs once
      while (!stop) {
        Long warcId = warcIdQueue.poll();
        if (warcId == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            log.error("Caught exception: ", e);
          }
          continue;
        }

        WarcProgressManager batch = getAndEnqueueWarc(warcId);
        if (batch != null) {
          log.info("Warc #{} retrieval complete. {} docs", warcId, batch.size());
        } else {
          log.error("Warc #{} was not indexed. Null response from Bamboo", warcId);
        }
      }
      log.info("Worker thread exiting.");
    }

    public void stop() {
      stop = true;
    }
  }
}