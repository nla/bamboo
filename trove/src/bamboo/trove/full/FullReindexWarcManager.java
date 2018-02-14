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
package bamboo.trove.full;

import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.task.WarcToIndex;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.ToIndex;
import bamboo.trove.common.WarcProgressManager;
import bamboo.trove.common.WarcSummary;
import bamboo.trove.db.FullPersistenceDAO;
import bamboo.trove.rule.RuleChangeUpdateManager;
import bamboo.trove.services.FilteringCoordinationService;
import bamboo.trove.services.JdbiService;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
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

import javax.annotation.PostConstruct;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

@Service
public class FullReindexWarcManager extends BaseWarcDomainManager {
  protected Logger log;
  private static final int POLL_INTERVAL_SECONDS = 1;
  private static final int ERROR_LIMIT = 0; // 5 is normal in prod. 0 is fastest but discards errors very aggressively
  private static final long TIMEOUT_ERROR_RETRY_MS = 5 * 60 * 1000; // 5 mins
  private static final long TIMEOUT_STALE_WARC_MS = 10 * 60 * 1000; // 10 mins

	@Autowired
	@Qualifier("solrDomainManager")
	private EndPointDomainManager solrManager;

	@Autowired
	private FilteringCoordinationService filteringCoordinationService;

	@Autowired
	private JdbiService database;

  // We don't really need this, but we want Spring to start it before us, so we list it as a dependency
  @SuppressWarnings("unused")
	@Autowired
	private RuleChangeUpdateManager ruleChangeUpdateManager;

  // Trove's DB stores IDs on where we are up to
  protected FullPersistenceDAO dao;
  private long persistedWarcId = 0;

  protected long bambooCollectionId = 3;
  protected int bambooBatchSize = 100;
  private int bambooReadThreads = 1;
  private String bambooCollectionsUrl;
  private int queueLimit = 5;
  // If not configured... will try stop seeking work
  // if there is less than 256mb of free heap  
  private long freeHeapLimit = 1024 * 1024 * 256;

  // Lifecycle states
  protected boolean running = false;
  private boolean starting = false;
  protected boolean stopping = false;

  protected boolean finishedFinding = false;
  private long warcsProcessed = 0;
  private long progressInBatchId = -1;
  private long endOfBatchId = -1;

  private WorkProcessor readPool;
  private List<ReadWorker> readWorkers = new ArrayList<>();
  // Queue of processing batches we are retrieving from Bamboo
  private Queue<ToIndex> warcIdQueue = new ConcurrentLinkedQueue<>();
  // Queue of batches we know are coming, but have not got full details yet
  private Queue<ToIndex> currentBatch;
  private boolean hasFinishedErrors = false;

  // We keep track of all batches until every object is either error'd or complete
  // This one is for persisting back to the database
  private Queue<Queue<ToIndex>> allBatches = new LinkedList<>();
  // Tracking batch information key'd by warc ID so we can run a dashboard
  private Map<Long, WarcProgressManager> warcTracking = new ConcurrentSkipListMap<>();
  private Map<Long, WarcSummary> warcSummaries = new ConcurrentSkipListMap<>();
  private Map<Long, Pair<Timestamp, Integer>> retryErrors = new ConcurrentHashMap<>();
  private Map<Long, FullPersistenceDAO.OldError> ignoredErrors = new ConcurrentHashMap<>();
  private Timer timer;

  private Map<Long, Integer> noSpamTimeout = new TreeMap<>();

  private int moduloDivisor = -1;
  private int moduloRemainder = -1;

  protected FullReindexWarcManager(){
  	log = LoggerFactory.getLogger(FullReindexWarcManager.class);
  }

  @SuppressWarnings("unused")
  public void setModuloDivisor(int moduloDivisor) {
    this.moduloDivisor = moduloDivisor;
    // If this is set to anything other than -1 (ie. has been explicitly turned off)
    // make sure we have not been asked to also managed rules.
    log.info("Distributed indexing! Divisor = {}", moduloDivisor);
    log.info("Distributed indexing! Rules updating = {}", ruleChangeUpdateManager.isDisableRulesUpdates());
    if (this.moduloDivisor != -1 && !ruleChangeUpdateManager.isDisableRulesUpdates()) {
      throw new IllegalArgumentException("Cannot set moduloDivisor (for distributed indexing)" +
              " because the 'disableRulesUpdates' flag has not been set.");
    }
  }

  @SuppressWarnings("unused")
  public void setModuloRemainder(int moduloRemainder) {
    this.moduloRemainder = moduloRemainder;
    log.info("Distributed indexing! Remainder = {}", moduloRemainder);
  }

  @SuppressWarnings("unused")
  public void setBambooBatchSize(int bambooBatchSize) {
    this.bambooBatchSize = bambooBatchSize;
  }

  @SuppressWarnings("unused")
  public void setBambooReadThreads(int bambooReadThreads) {
    this.bambooReadThreads = bambooReadThreads;
  }

  @SuppressWarnings("unused")
  public void setQueueLimit(int queueLimit) {
    this.queueLimit = queueLimit;
  }

  @SuppressWarnings("unused")
  public void setFreeHeapLimit(long freeHeapLimit) {
    this.freeHeapLimit = freeHeapLimit;
    BaseWarcDomainManager.setFreeHeapLimitParser(freeHeapLimit);
  }

  @SuppressWarnings("unused")
  public Map<Long, WarcSummary> getBatchMap() {
    return warcSummaries;
  }

  @SuppressWarnings("unused")
  public Map<Long, FullPersistenceDAO.OldError> getIgnoredErrors() {
    return ignoredErrors;
  }

  @Required
  public void setBambooCollectionId(long bambooCollectionId) {
    this.bambooCollectionId = bambooCollectionId;
  }

  @PostConstruct
  public void init() throws InterruptedException {
    init(true);
  }
  protected void init(boolean createMetrics) throws InterruptedException {
    waitUntilStarted();
    log.info("***** FullReindexWarcManager *****");
    bambooCollectionsUrl = getBambooApiBaseUrl() + "collections/" + bambooCollectionId + "/warcs/json";

    log.info("Bamboo Collection : {}", bambooCollectionsUrl);
    log.info("Warc read threads : {}", bambooReadThreads);
    log.info("Warc queue limit  : {}", queueLimit);
    log.info("Run at start      : {}", runAtStart);

    dao = database.getDao().fullPersistence();
    persistedWarcId = dao.getLastId(moduloRemainder);
    endOfBatchId = persistedWarcId;

    if(createMetrics){
      MetricRegistry metrics = SharedMetricRegistries.getOrCreate(filteringCoordinationService.getMetricsRegistryName());
      Gauge<Long> gaugeQueue = () -> (long) (warcTracking.size() + warcIdQueue.size());
      metrics.register("queueLength", gaugeQueue);
      Gauge<Long> gaugeHeap = () -> (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory())
              + Runtime.getRuntime().freeMemory();
      metrics.register("freeHeap", gaugeHeap);
      // These don't change at runtime, but we'll use the same plumbing to get them to the UI
      Gauge<Long> gaugeQueueLimit = () -> (long) queueLimit;
      metrics.register("queueLengthLimit", gaugeQueueLimit);
      Gauge<Long> gaugeHeapLimit = () -> freeHeapLimit;
      metrics.register("freeHeapLimit", gaugeHeapLimit);
    }
    
    List<FullPersistenceDAO.OldError> oldErrors = dao.oldErrors();
    LinkedList<ToIndex> errorList = new LinkedList<>();

    for (FullPersistenceDAO.OldError e : oldErrors) {
      if (e.error.getSecond() < ERROR_LIMIT) {
        WarcToIndex warc = new WarcToIndex();
        warc.setId(e.warcId);
        log.info("Old error for warc {} found. Marking for retry.", e.warcId);
        errorList.offer(new ToIndex(warc));
      } else {
        ignoredErrors.put(e.warcId, e);
      }
    }
    if(!errorList.isEmpty()){
    	currentBatch = errorList;
    }

    readPool = new WorkProcessor(bambooReadThreads);
  }

  private void startWorkers() {
    for (int i = 0; i < bambooReadThreads; i++) {
      ReadWorker worker = new ReadWorker();
      readWorkers.add(worker);
      readPool.process(worker);
    }
  }

  protected void stopWorkers() {
    // Tell each worker to stop
    int size = readWorkers.size();
    for (int i = 0; i < size; i++) {
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
    if (!running && !starting && !stopping)  {
      starting = true;
    	// use new thread to start so we don't block on the lock and leave the ui hanging.
    	Thread thread = new Thread(()->{      
    		acquireDomainStartLock();
        try {
          if (!running && starting && !stopping)  {
            startInnner();
            starting = false;
          }
        } finally {
          releaseDomainStartLock();
        }
      });
    	thread.start();
    }
  }

  public void startInnner() {
    log.info("Starting...");
    running = true;
    tick();

    Thread me = new Thread(this);
    me.setName(getName());
    me.start();
  }

  public void run() {
    startWorkers();
    loop();
  }

  @Override
  public void stop() {
  	stopInner();
  }
  
  public void stopInner() {
    if (running && !stopping)  {
      stopping = true;
      log.info("Stopping domain... {} Bamboo read threads still need to stop", readWorkers.size());
      stopWorkers();
      log.info("All workers stopped!");

      if (timer != null) {
        log.info("Cancelling batch management timer");
        timer.cancel();
        timer = null;
      }

      running = false;
      stopping = false;
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


  protected void loop() {
    while (running && !stopping && !finishedFinding) {
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

  protected LinkedList<ToIndex> getNextBatch() throws IOException {
    long startOfNextBatch = endOfBatchId + 1;
    URL url = new URL(bambooCollectionsUrl + "?start=" + startOfNextBatch + "&rows=" + bambooBatchSize);
    log.info("Contacting Bamboo for more IDs. start={}, rows={}", startOfNextBatch, bambooBatchSize);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty("Accept-Encoding", "gzip");

    InputStream in = new BufferedInputStream(connection.getInputStream());
    if("gzip".equals(connection.getHeaderField("Content-Encoding"))){
    	in = new GZIPInputStream(in);
    }

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
      ToIndex nextWarc = new ToIndex(om.readValue(json, WarcToIndex.class));
      if (moduloDivisor != -1 && moduloRemainder != -1) {
        // We are only doing some things
        if ((nextWarc.getId() % moduloDivisor) == moduloRemainder) {
          result.add(nextWarc);
        }

      // We are doing everything
      } else {
        result.add(nextWarc);
      }
    }

    if (moduloDivisor != -1 && moduloRemainder != -1) {
      log.info("Received {} objects from Bamboo.", result.size());
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
    	 hasFinishedErrors = true;
       // Get a new batch
       LinkedList<ToIndex> newBatch = getNextBatchWithRetry();
       if (newBatch == null || newBatch.isEmpty()) {
         log.info("Retrieved empty batch from Bamboo. Work completed.");
         return true;
       }
       // Including a separate reference just to ensure we know when to persist back to the DB
       LinkedList<ToIndex> persistTracking = new LinkedList<>();
       persistTracking.addAll(newBatch);
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

  protected void doWork() throws InterruptedException, IOException {
    // Work complete
    if (checkWorkComplete()) {
      finishedFinding = true;
      return;
    }

    // Saturated queue?
    int queueSize = (warcTracking.size() + warcIdQueue.size());
    if (queueSize >= queueLimit) {
      Thread.sleep(1000);
      return;
    }

    // Saturated heap? 
    long unusedHeap = (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory())
            + Runtime.getRuntime().freeMemory();
//    if (queueSize >= 10 && unusedHeap < freeHeapLimit) {
    if (unusedHeap < freeHeapLimit) {
      Thread.sleep(1000);
      return;
    }

    // greg said We should log this
    // as is not in the dash board.
    if(!hasFinishedErrors){
    	log.info("Still have {} error warcs to process.", currentBatch.size());
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
      stopInner();
    }

  }

  private boolean trackError(WarcProgressManager warc) {
    // Remember an instantiation of a WarcProgressManager is a single attempt for a warc to be indexed.
    // On a retry attempt it will be a new object, so never track the same _instantiation_ twice.
    if (warc.isTrackedError()) return false;

    // Persist into the database.
    // This will update timestamp and (if required) bump the retry counter
    updateTrackError(warc.getWarcId());
    FullPersistenceDAO.OldError errorData = dao.checkError(warc.getWarcId());
    warc.trackedError(errorData.error);

    if (errorData.error.getSecond() < ERROR_LIMIT) {
      // Schedule for a retry
      retryErrors.put(warc.getWarcId(), errorData.error);
      return false;
    } else {
      // Remove it entirely
      ignoredErrors.put(warc.getWarcId(), errorData);
      return true;
    }
  }

  protected void updateTrackError(long id){
    dao.trackError(id, FullPersistenceDAO.Domain.FULL.getCode());
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

      // Branch 1 ... the work is 'finished'
      if (warc.finished()) {
        // State = Healthy
        if (warc.finishedWithoutError()) {
          completedWarcs.add(key);
          dao.removeError(key);
          warcsProcessed++;
          if (key > progressInBatchId) {
            progressInBatchId = key;
          }

        // State = Error
        } else {
          if (trackError(warc)) {
            completedWarcs.add(key);
          }
        }

      // Branch 2 ... why has it not finished?
      } else {
        // State = Error
        if (warc.isLoadingFailed()) {
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
    completedWarcs.addAll(ignoredErrors.keySet().stream()
            .filter(warcId -> warcTracking.containsKey(warcId))
            .collect(Collectors.toList()));

    // Cleanup
    for (Long warcId : completedWarcs) {
      //log.info("De-referencing completed warc: {}", warcId);
      WarcProgressManager warc = warcTracking.get(warcId);
      if (warc != null) {
        warc.mothball();
      }
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
    ToIndex warcIndexing = null;

    // Until we find something still active, keep trying
    while (keepGoing) {
      ToIndex warcToIndex = iHopeThisIsDone.peek();
      if (warcToIndex == null) {
        itIsDone = true;
        keepGoing = false;
        continue;
      }
      warcIndexing = warcToIndex;
      if (!warcToIndex.hasBeenRetrieved) {
        // We haven't indexed this far yet!
        keepGoing = false;
        continue;
      }

      // If it is still being tracked...
      if (warcTracking.containsKey(warcIndexing.getId())) {
        WarcProgressManager warc = warcTracking.get(warcIndexing.getId());
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
      if (warcIndexing != null) {
        updateLastId(warcIndexing);
        persistedWarcId = warcIndexing.getId();
        log.info("Persisting progress for ID '{}'. Currently monitoring {} batches", warcIndexing.getId(), allBatches.size());
      }
      // Clear it from the head
      allBatches.poll();
    }
  }

  protected void updateLastId(ToIndex warc){
    dao.updateLastId(warc.getId(), moduloRemainder);
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

        WarcProgressManager batch = getAndEnqueueWarc(toIndex);
        if (batch != null) {
          log.trace("Warc #{} retrieval complete. {} docs, estimated {}",
                  toIndex.getId(), batch.size(), toIndex.getUrlCount());
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
}
