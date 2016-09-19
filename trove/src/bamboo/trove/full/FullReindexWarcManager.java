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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.PostConstruct;

import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.task.WarcToIndex;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.WarcProgressManager;
import bamboo.trove.common.WarcSummary;
import bamboo.trove.db.FullPersistenceDAO;
import bamboo.trove.services.FilteringCoordinationService;
import bamboo.trove.services.JdbiService;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.util.Pair;
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
  private static final long TIMEOUT_ERROR_RETRY_MS = 15 * 60 * 1000; // 15 mins
  private static final long TIMEOUT_STALE_WARC_MS = 10 * 60 * 1000; // 10 mins

	@Autowired
	@Qualifier("solrDomainManager")
	private EndPointDomainManager solrManager;

	@Autowired
	private FilteringCoordinationService filteringService;

	@Autowired
	private JdbiService database;

  // Trove's DB stores IDs on where we are up to
  private FullPersistenceDAO dao;
  private long persistedWarcId = 0;

  private long bambooCollectionId = 3;
  private int bambooBatchSize = 100;
  private int bambooReadThreads = 1;
  private String bambooBaseUrl;
  private String bambooCollectionsUrl;
  private int queueLimit = 5;

  private int maxFilterWorkers;
  private int maxTransformWorkers;
  private int maxIndexWorkers;

  private boolean running = false;
  private boolean stopping = false;
  private boolean finishedFinding = false;
  private long warcsProcessed = 0;
  private long progressInBatchId = -1;
  private long endOfBatchId = -1;

  private WorkProcessor readPool;
  private List<ReadWorker> readWorkers = new ArrayList<>();
  // Queue of processing batches we are retrieving from Bamboo
  private Queue<ToIndex> warcIdQueue = new ConcurrentLinkedQueue<>();
  // Queue of batches we know are coming, but have not got full details yet
  private Queue<ToIndex> currentBatch;

  // We keep track of all batches until every object is either error'd or complete
  // This one is for persisting back to the database
  private Queue<Queue<ToIndex>> allBatches = new LinkedList<>();
  // Tracking batch information key'd by warc ID so we can run a dashboard
  private Map<Long, WarcProgressManager> warcTracking = new ConcurrentSkipListMap<>();
  private Map<Long, WarcSummary> warcSummaries = new ConcurrentSkipListMap<>();
  private Map<Long, Pair<Timestamp, Integer>> retryErrors = new ConcurrentHashMap<>();
  private Map<Long, Pair<Timestamp, Integer>> ignoredErrors = new ConcurrentHashMap<>();
  private Timer timer;

  private Map<Long, Integer> noSpamTimeout = new TreeMap<>();

  public void setBambooBatchSize(int bambooBatchSize) {
    this.bambooBatchSize = bambooBatchSize;
  }

  public void setBambooReadThreads(int bambooReadThreads) {
    this.bambooReadThreads = bambooReadThreads;
  }

  public void setQueueLimit(int queueLimit) {
    this.queueLimit = queueLimit;
  }

  public Map<Long, WarcSummary> getBatchMap() {
    return warcSummaries;
  }

  public Map<Long, Pair<Timestamp, Integer>> getIgnoredErrors() {
    return ignoredErrors;
  }

  @Required
  public void setBambooBaseUrl(String bambooBaseUrl) {
    this.bambooBaseUrl = bambooBaseUrl;
  }

  @Required
  public void setBambooCollectionId(long bambooCollectionId) {
    this.bambooCollectionId = bambooCollectionId;
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
    // The core Trove indexer doesn't really match the model we have here were all of the domains share worker pools,
    // so this startup pattern will look a little odd to align with that view of the work. This domain will configure
    // and init (via statics) the base class all of the other domains extend. They will wait until we are done.
    BaseWarcDomainManager.setBambooApiBaseUrl(bambooBaseUrl);
    BaseWarcDomainManager.setWorkerCounts(maxFilterWorkers, maxTransformWorkers, maxIndexWorkers);
    BaseWarcDomainManager.startMe(solrManager, filteringService);
    bambooCollectionsUrl = bambooBaseUrl + "collections/" + bambooCollectionId + "/warcs/json";

    log.info("Bamboo Collection : {}", bambooCollectionsUrl);
    log.info("Warc read threads : {}", bambooReadThreads);
    log.info("Warc queue limit  : {}", queueLimit);
    log.info("Run at start      : {}", runAtStart);

    dao = database.getDao().fullPersistence();
    persistedWarcId = dao.getLastId();
    endOfBatchId = persistedWarcId;

    List<FullPersistenceDAO.OldError> oldErrors = dao.oldErrors();
    for (FullPersistenceDAO.OldError e : oldErrors) {
      if (e.error.getSecond() < 5) {
        WarcToIndex warc = new WarcToIndex();
        warc.setId(e.warcId);
        log.info("Old error for warc {} found. Marking for retry.", e.warcId);
        warcIdQueue.offer(new ToIndex(warc));
      } else {
        ignoredErrors.put(e.warcId, e.error);
      }
    }

    readPool = new WorkProcessor(bambooReadThreads);

    tick();
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
      log.info("Stopping domain... {} Bamboo read threads still need to stop", readWorkers.size());
      stopping = true;
      stopWorkers();
      log.info("All workers stopped!");

      if (timer != null) {
        log.info("Cancelling batch management timer");
        timer.cancel();
      }
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
    return "warc#" + progressInBatchId + " (#" + persistedWarcId + " has been persisted)";
  }


  private void loop() {
    while (running && !stopping&& !finishedFinding) {
      try {
        try {
          doWork();

        } catch (IOException e) {
          log.error("Error talking to Bamboo. Waiting 5 minutes before trying again: '{}'", e.getMessage());
          // Try again in 5 minutes
          Thread.sleep(5 * 60 * 1000);

        } catch (Exception e) {
          log.error("Unexpected error during doWork(). Waiting 1 hour before trying again: ", e);
          Thread.sleep(60 * 60 * 1000);
        }
      } catch (InterruptedException e) {
        log.error("Thread sleep interrupted whilst waiting on batch completion. Resuming: {}", e.getMessage());
      }
    }
  }

  private LinkedList<ToIndex> getNextBatch() throws IOException {
    long startOfNextBatch = endOfBatchId + 1;
    URL url = new URL(bambooCollectionsUrl + "?start=" + startOfNextBatch + "&rows=" + bambooBatchSize);
    log.info("Contacting Bamboo for more IDs. start={}, rows={}", startOfNextBatch, bambooBatchSize);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    InputStream in = new BufferedInputStream(connection.getInputStream());

    ObjectMapper om = getObjectMapper();
    JsonParser json = createParser(in);
    JsonToken token = json.nextToken();

    if (token == null) {
      throw new IllegalArgumentException("No JSON data found in response");
    }
    if (!JsonToken.START_ARRAY.equals(token)) {
      throw new IllegalArgumentException("JSON response is not an array");
    }

    LinkedList<ToIndex> result = new LinkedList<>();
    while (json.nextToken() == JsonToken.START_OBJECT) {
      result.add(new ToIndex(om.readValue(json, WarcToIndex.class)));
    }
    return result;
  }

  private LinkedList<ToIndex> getNextBatchWithRetry() throws InterruptedException, IOException {
    try {
      return getNextBatch();
    } catch (IOException e) {
      log.warn("Error talking to Bamboo during batch retrieval. Retrying in 10s. Msg was '{}'", e.getMessage());
      Thread.sleep(10000);
      return getNextBatch();
    }
  }

  private boolean checkWorkComplete() throws InterruptedException, IOException {
     if (currentBatch == null || currentBatch.isEmpty()) {
       // Get a new batch
       LinkedList<ToIndex> newBatch = getNextBatchWithRetry();
       if (newBatch == null || newBatch.isEmpty()) {
         log.info("Retrieved empty batch from Bamboo. Work completed.");
         return true;
       }
       // Including a separate reference just to ensure we know when to persist back to the DB
       LinkedList<ToIndex> persistTracking = new LinkedList<>();
       for (ToIndex w : newBatch) {
         persistTracking.add(w);
       }
       allBatches.add(persistTracking);
       // Update state
       endOfBatchId = newBatch.peekLast().getId();
       currentBatch = newBatch;
       return false;

     } else {
       // We are still in this batch
       return false;
     }
  }

  private void doWork() throws InterruptedException, IOException {
    // Work complete
    if (checkWorkComplete()) {
      finishedFinding = true;
      return;
    }

    // Saturated queue
    if ((warcTracking.size() + warcIdQueue.size()) >= queueLimit) {
      Thread.sleep(1000);
      return;
    }

    warcIdQueue.offer(currentBatch.poll());
  }

  private void setTick() {
    if (timer == null) {
      timer = new Timer();
    }
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        tick();
      }
    }, POLL_INTERVAL_SECONDS * 1000);
  }

  private void logTimeout(long key, WarcProgressManager warc) {
    log.warn("Warc {} has been pending for a long time. {} of {} URLs, F#{}, T#{}, I#{}", key, warc.size(),
            warc.getUrlCountEstimate(), warc.getCountFilterCompleted(), warc.getCountTransformCompleted(),
            warc.getCountIndexCompleted());
  }

  private void tick() {
    try {
      checkBatches();

    } catch (Exception e) {
      log.error("Unexpected error during checkBatches(): ", e);
    }

    // Keep going unless we are done
    if (!(finishedFinding && warcTracking.isEmpty() && allBatches.isEmpty())) {
      setTick();
    } else {
      stop();
    }

  }

  private boolean trackError(WarcProgressManager warc) {
    // Remember an instantiation of a WarcProgressManager is a single attempt for a warc to be indexed.
    // On a retry attempt it will be a new object, so never track the same _instantiation_ twice.
    if (warc.isTrackedError()) return false;

    // Persist into the database.
    // This will update timestamp and (if required) bump the retry counter
    dao.trackError(warc.getWarcId());
    Pair<Timestamp, Integer> errorData = dao.checkError(warc.getWarcId());
    warc.trackedError(errorData);

    if (errorData.getSecond() < 5) {
      // Schedule for a retry
      retryErrors.put(warc.getWarcId(), errorData);
      return false;
    } else {
      // Remove it entirely
      ignoredErrors.put(warc.getWarcId(), errorData);
      return true;
    }
  }

  private void checkBatches() {
    long now = new Date().getTime();
    // Check state
    List<Long> completedWarcs = new ArrayList<>();
    for (Long key : warcTracking.keySet()) {
      WarcProgressManager warc = warcTracking.get(key);
      // This happened... it was an annoying bug
      if (warc == null) {
        throw new IllegalStateException("Warc ID " + key + " is null in the tracking map. This is a logic bug");
      }
      // State = Healthy
      if (warc.finishedWithoutError()) {
        completedWarcs.add(key);
        dao.removeError(key);
        warcsProcessed++;
        if (key > progressInBatchId) {
          progressInBatchId = key;
        }

      } else {
        // State = Error
        if (warc.finished() || warc.isLoadingFailed()) {
          if (trackError(warc)) {
            completedWarcs.add(key);
          }

        // State = Stale?
        } else {
          // X minutes without completion
          if ((now - warc.getTimeStarted()) > TIMEOUT_STALE_WARC_MS) {
            // First time
            if (!noSpamTimeout.containsKey(key)) {
              logTimeout(key, warc);
              noSpamTimeout.put(key, 0);
            // Or every 5 minutes
            } else {
              int was = noSpamTimeout.get(key);
              if (was > (300 / POLL_INTERVAL_SECONDS)) {
                logTimeout(key, warc);
                noSpamTimeout.put(key, 0);
              } else {
                noSpamTimeout.put(key, was + 1);
              }
            }
          }
        }
      }
    }

    // Before cleanup, check the error retry queue and consider moving warcs around based on that
    // After a queue starts a retry attempt we can add it to the cleanup
    Iterator<Map.Entry<Long, Pair<Timestamp, Integer>>> itr = retryErrors.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Long, Pair<Timestamp, Integer>> entry = itr.next();
      long warcId = entry.getKey();
      long lastError = entry.getValue().getFirst().getTime();
      if ((now - lastError) > TIMEOUT_ERROR_RETRY_MS) {
        log.info("Retrying warc {}. last attempt = {}, now = {}, diff = {}", warcId, lastError, now, (now - lastError));
        // Find the warc
        WarcProgressManager warc = warcTracking.get(warcId);
        // Put it back into the queue as a fresh object (retaining a reference to the old instance)
        // Note. We don't yet use the old reference, but I'm retaining it for now in case we decide we want it later
        // The DB layer retains a minimal 'history' to the object for errors anyway, but at least until a restart
        // the old reference will give us a complete error history.
        warcIdQueue.offer(new ToIndex(warc));
        // Stop considering it for retries
        itr.remove();
        // Make sure the old tracking instance of the warc is cleaned up
        completedWarcs.add(warcId);
      }
    }

    // Cleaning up ignored errors is even simpler... just make sure we aren't tracking them still
    for (Long warcId : ignoredErrors.keySet()) {
      if (warcTracking.containsKey(warcId)) {
        completedWarcs.add(warcId);
      }
    }

    // Cleanup
    for (Long warcId : completedWarcs) {
      //log.info("De-referencing completed warc: {}", warcId);
      warcTracking.get(warcId).mothball();
      warcTracking.remove(warcId);
      warcSummaries.remove(warcId);
    }

    checkPersistence();
  }

  private void checkPersistence() {
    // Persist progress back to the database if we can
    LinkedList<ToIndex> iHopeThisIsDone = (LinkedList<ToIndex>) allBatches.peek();
    if (iHopeThisIsDone == null) return;

    boolean itIsDone = false;
    boolean keepGoing = true;
    long warcId = 0;

    // Until we find something still active, keep trying
    while (keepGoing) {
      ToIndex warcToIndex = iHopeThisIsDone.peek();
      if (warcToIndex == null) {
        itIsDone = true;
        keepGoing = false;
        continue;
      }
      warcId = warcToIndex.getId();
      if (!warcToIndex.hasBeenRetrieved) {
        // We haven't indexed this far yet!
        keepGoing = false;
        continue;
      }

      // If it is still being tracked...
      if (warcTracking.containsKey(warcId)) {
        WarcProgressManager warc = warcTracking.get(warcId);
        // It might only be tracked because of errors... which are persisted separately
        if (warc.finished() && warc.hasErrors()) {
          iHopeThisIsDone.poll();

        } else {
          // There is work left in this batch. Stop checking
          keepGoing = false;
        }

      // Not tracked. This warc is done
      } else {
        iHopeThisIsDone.poll();
      }
    }

    // All warcs are completed in this batch
    if (itIsDone) {
      dao.updateLastId(warcId);
      persistedWarcId = warcId;
      log.info("Persisting progress for ID '{}'. Currently monitoring {} batches", warcId, allBatches.size());
      // Clear it from the head
      allBatches.poll();
    }
  }

  @Override
  protected WarcProgressManager newWarc(long warcId, long trackedOffset, long urlCountEstimate) {
    WarcProgressManager newWarc = new WarcProgressManager(warcId, trackedOffset, urlCountEstimate);
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
        ToIndex toIndex = warcIdQueue.poll();
        if (toIndex == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            log.error("Caught exception: ", e);
          }
          continue;
        }

        WarcProgressManager batch = getAndEnqueueWarc(toIndex.getId(), toIndex.getUrlCount());
        if (batch != null) {
          //log.info("Warc #{} retrieval complete. {} docs, estimated {}",
          //        toIndex.getId(), batch.size(), toIndex.getUrlCount());
        } else {
          log.error("Warc #{} was not indexed. Null response from Bamboo", toIndex.getId());
        }
        // Load Failed will be set in error cases, so 'retrieval' still occurred for tracking purposes
        toIndex.hasBeenRetrieved = true;
      }
      log.info("Stop received! Bamboo read thread exiting.");
    }

    public void stop() {
      stop = true;
    }
  }

  private class ToIndex extends WarcToIndex {
    private boolean hasBeenRetrieved = false;
    private WarcProgressManager oldWarcInstance = null;

    // A normal event from Bamboo
    public ToIndex(WarcToIndex warc) {
      setId(warc.getId());
      setUrlCount(warc.getUrlCount());
    }

    // Something from the retry queue
    public ToIndex(WarcProgressManager warc) {
      setId(warc.getWarcId());
      setUrlCount(warc.getUrlCountEstimate());
      oldWarcInstance = warc;
    }

    public boolean isRetryAttempt() {
      return !(oldWarcInstance == null);
    }

    public WarcProgressManager oldAttempt() {
      return oldWarcInstance;
    }
  }
}