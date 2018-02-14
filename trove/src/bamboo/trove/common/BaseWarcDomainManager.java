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

import au.gov.nla.trove.indexer.api.BaseDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.task.Document;
import bamboo.trove.services.FilteringCoordinationService;
import bamboo.trove.services.RankingService;
import bamboo.trove.workers.FilterWorker;
import bamboo.trove.workers.IndexerWorker;
import bamboo.trove.workers.TransformWorker;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;

public abstract class BaseWarcDomainManager extends BaseDomainManager implements Runnable {
  private static Logger log = LoggerFactory.getLogger(BaseWarcDomainManager.class);

	protected boolean runAtStart;
  @Required
  public void setRunAtStart(boolean runAtStart) {
    this.runAtStart = runAtStart;
  }
  private static long freeHeapLimit = 0;
  public static void setFreeHeapLimitParser(long freeHeapLimit){
    BaseWarcDomainManager.freeHeapLimit = freeHeapLimit;
  }

  // Reading data from Bamboo
  private static Timer bambooReadTimer; 
  private static Timer bambooParseTimer;
  private static long bambooCacheNullLong = 0;
  private static long bambooCacheHitLong = 0;
  private static long bambooCacheMissLong = 0;
  private static Histogram warcDocCountHistogram;
  private static Histogram warcSizeHistogram;
  private static String bambooBaseUrl;
  private ObjectMapper objectMapper = new ObjectMapper();
  private JsonFactory jsonFactory = new JsonFactory();

  // self registering list of domains
  private static final List<BaseWarcDomainManager> domainsList = new ArrayList<>(); 

  // Managing pool tasks
  private static Queue<IndexerDocument> filterQueue = new ConcurrentLinkedQueue<>();
  private static Queue<IndexerDocument> transformQueue = new ConcurrentLinkedQueue<>();
  private static Queue<IndexerDocument> indexQueue = new ConcurrentLinkedQueue<>();

  // Performing pool tasks
  private static int filterPoolLimit;
  private static int transformPoolLimit;
  private static int indexPoolLimit;
  
  public static RankingService rankingService;

  private static void notAlreadyStarted() {
    if (imStarted) {
      throw new IllegalStateException("The domain has already started!");
    }
  }
  protected static void setBambooApiBaseUrl(String newBambooBaseUrl) {
    notAlreadyStarted();
    bambooBaseUrl = newBambooBaseUrl;
  }
  protected static String getBambooApiBaseUrl() {
    return bambooBaseUrl;
  }
  protected static void setWorkerCounts(int filters, int transformers, int indexers) {
    notAlreadyStarted();
    filterPoolLimit = filters;
    transformPoolLimit = transformers;
    indexPoolLimit = indexers;
  }

  // This could be more elegant... but rather that have static init() code here that runs itself we
  // want to get config from a particular domain. This is solely to be consistent with how the standard
  // Trove indexer works. So here we want all domains to park and wait until after the 'full' domain has
  // started the shared worker pools.
  private static boolean imStarted = false;
  protected static synchronized void startMe(FilteringCoordinationService filtering, boolean indexFullText) {
    if (imStarted) {
      throw new IllegalStateException("You started me twice!");
    }
		log.info("Bamboo Base URL  : {}", bambooBaseUrl);
		log.info("Metrics registry : {}", filtering.getMetricsRegistryName());
		log.info("# Filters        : {}", filterPoolLimit);
		log.info("# Transformers   : {}", transformPoolLimit);
		log.info("# Indexers       : {}", indexPoolLimit);

    // Metrics are fun...
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(filtering.getMetricsRegistryName());
    bambooReadTimer = new Timer();
    metrics.register("bambooReadTimer", bambooReadTimer);
    bambooParseTimer = new Timer();
    metrics.register("bambooParseTimer", bambooParseTimer);
    Gauge<Long> bambooCacheNull = () -> bambooCacheNullLong;
    metrics.register("bambooCacheNull", bambooCacheNull);
    Gauge<Long> bambooCacheHit = () -> bambooCacheHitLong;
    metrics.register("bambooCacheHit", bambooCacheHit);
    Gauge<Long> bambooCacheMiss = () -> bambooCacheMissLong;
    metrics.register("bambooCacheMiss", bambooCacheMiss);
    warcDocCountHistogram = new Histogram(new UniformReservoir());
    metrics.register("warcDocCountHistogram", warcDocCountHistogram);
    warcSizeHistogram = new Histogram(new UniformReservoir());
    metrics.register("warcSizeHistogram", warcSizeHistogram);
    Timer filterTimer = new Timer();
    metrics.register("filterTimer", filterTimer);
    Timer transformTimer = new Timer();
    metrics.register("transformTimer", transformTimer);
    Timer indexTimer = new Timer();
    metrics.register("indexTimer", indexTimer);

    // Filter workers
    WorkProcessor filterPool = new WorkProcessor(filterPoolLimit);
    for (int i = 0; i < filterPoolLimit; i++) {
      filterPool.process(new FilterWorker(filtering, filterTimer));
    }

    // Transform workers
    WorkProcessor transformPool = new WorkProcessor(transformPoolLimit);
    for (int i = 0; i < transformPoolLimit; i++) {
      transformPool.process(new TransformWorker(transformTimer, indexFullText));
    }

    // Indexing workers
    WorkProcessor indexPool = new WorkProcessor(indexPoolLimit);
    for (int i = 0; i < indexPoolLimit; i++) {
      indexPool.process(new IndexerWorker(indexTimer));
    }

    imStarted = true;
  }

  private static final ReentrantLock DOMAIN_START_LOCK = new ReentrantLock();
  protected static void acquireDomainStartLock() {
    DOMAIN_START_LOCK.lock();
  }
  protected static void releaseDomainStartLock() {
    // Only the holding thread is allowed to call this,
    // otherwise an IllegalMonitorStateException will be thrown
    DOMAIN_START_LOCK.unlock();
  }

  @VisibleForTesting
  @SuppressWarnings("unused")
  public static void forTestSetMetricsRegistryName(String metricsRegistryName) {
    if (imStarted) {
      throw new IllegalStateException("Unit tests only!!!");
    }
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(metricsRegistryName);
    bambooReadTimer = new Timer();
    metrics.register("bambooReadTimer", bambooReadTimer);
    bambooParseTimer = new Timer();
    metrics.register("bambooParseTimer", bambooParseTimer);
    warcDocCountHistogram = new Histogram(new UniformReservoir());
    metrics.register("warcDocCountHistogram", warcDocCountHistogram);
    warcSizeHistogram = new Histogram(new UniformReservoir());
    metrics.register("warcSizeHistogram", warcSizeHistogram);
  }

  @VisibleForTesting
  @SuppressWarnings("unused")
  public static void forTestSetBambooBaseUrl(String bambooBaseUrl) {
    if (imStarted) {
      throw new IllegalStateException("Unit tests only!!!");
    }
    BaseWarcDomainManager.bambooBaseUrl = bambooBaseUrl;
  }

  protected static List<BaseWarcDomainManager> getDomainList(){
  	// wait for domains to register
  	// this code is needed to allow the domains to start but hide the circular dependency
  	// and spring not able to control the start order.
  	long timeoutSec = 20;
  	long timeout = System.currentTimeMillis() + (timeoutSec*1000);
  	while(domainsList.size()<3){
      try{
      	if(System.currentTimeMillis() > timeout){
      		throw new IllegalStateException("Failed to register domains after "+timeoutSec+" secs.");
      	}
				Thread.sleep(1000);
			}
			catch (InterruptedException e){
				// ignore
			}
      catch(IllegalStateException e){
      	log.error("Error getting list of domains. Stop processing", e);
      	System.exit(1);
      }
  	}
  	return domainsList;
  }

  protected void waitUntilStarted() throws InterruptedException {
  	domainsList.add(this);
    while (!imStarted) {
      Thread.sleep(1000);
    }
  }

  @Override
  public abstract boolean isRunning();

  @Override
  public abstract boolean isStopping();

  @Override
  public abstract void start();

  @Override
  public abstract void stop();

  @Override
  public abstract String getName();

  @Override
  public abstract long getUpdateCount();

  @Override
  public abstract String getLastIdProcessed();

  public static IndexerDocument getNextFilterJob(IndexerDocument lastJob) {
    if (lastJob != null) {
      transformQueue.offer(lastJob);
    }
    return filterQueue.poll();
  }

  public static IndexerDocument getNextTransformJob(IndexerDocument lastJob) {
    if (lastJob != null) {
      indexQueue.offer(lastJob);
    }
    return transformQueue.poll();
  }

  @SuppressWarnings("unused")
  public static IndexerDocument getNextIndexJob(IndexerDocument lastJob) {
    return indexQueue.poll();
  }

  // Full domain overrides this
  protected WarcProgressManager newWarc(long warcId, long trackedOffset, long urlCountEstimate) {
    return new WarcProgressManager(warcId, trackedOffset, urlCountEstimate);
  }

  public WarcProgressManager getAndEnqueueWarc(ToIndex warcToIndex) {
    Timer.Context ctx = bambooReadTimer.time();
    HttpURLConnection connection = null;
    Long warcId = warcToIndex.getId();
    WarcProgressManager warc = newWarc(warcId, warcToIndex.getTrackedOffset(), warcToIndex.getUrlCount());

    try {
      String urlString = bambooBaseUrl + "warcs/" + warcId + "/text?tika=1&pdfbox=1";
      if (warcToIndex.isRetryAttempt()) {
        log.info("Forcing cache bypass for warc #{} because of error retry.", warcId);
        urlString += "&bypass=1";
      }
      URL url = new URL(urlString);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Accept-Encoding", "gzip");

      String cacheStatus = connection.getHeaderField("X-Cache-Status");
      // HIT is most likely when the cache is full and we are the bottleneck. test first
      if ("HIT".equals(cacheStatus)) {
        bambooCacheHitLong++;
      } else {
        // When the cache isn't full this will be normal. Bamboo will be the bottleneck
        if ("MISS".equals(cacheStatus) || "BYPASS".equals(cacheStatus)) {
          bambooCacheMissLong++;
        } else {
          // We don't really expect with of these.
          if (cacheStatus == null) {
            bambooCacheNullLong++;
          } else {
            log.error("Received unexpected cache header value: '{}'", cacheStatus);
            throw new IllegalArgumentException("Unexpected response from Bamboo caching layer");
          }
        }
      }

      InputStream in = new BufferedInputStream(connection.getInputStream());
      if("gzip".equals(connection.getHeaderField("Content-Encoding"))){
        in = new GZIPInputStream(in);
      }

      parseJson(warc, in);
      warc.setLoadComplete();
      return warc;

    } catch (IOException e) {
      log.error("Error talking to Bamboo (warc#{}): {}", warcId, e.getMessage());
      warc.setLoadFailed();
      return null;

    } catch (Exception e) {
      log.error("Unknown error getting data from Bamboo: {}", e.getMessage());
      warc.setLoadFailed();
      return null;

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
      ctx.stop();
    }
  }

  protected ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  protected JsonParser createParser(InputStream in) throws IOException {
    return jsonFactory.createParser(in);
  }

  // Important to note that this stream based parsing works very simple because to the POJO's being parsed
  // are very simple. If the Document class became more complicated this method would have to be as well.
  private void parseJson(WarcProgressManager warc, InputStream in) throws IOException {
    Timer.Context ctx = bambooParseTimer.time();
    JsonParser json = createParser(in);
    JsonToken token = json.nextToken();
    if (token == null) {
      ctx.stop();
      throw new IllegalArgumentException("No JSON data found in response");
    }
    if (!JsonToken.START_ARRAY.equals(token)) {
      ctx.stop();
      throw new IllegalArgumentException("JSON response is not an array");
    }

    try {
      long warcSize = 0;
      while (json.nextToken() == JsonToken.START_OBJECT) {
        Document d = objectMapper.readValue(json, Document.class);
        warcSize += d.getContentLength();
        // Track it by batch
        IndexerDocument doc = warc.add(d);
        // Enqueue it for work
        filterQueue.offer(doc);
        // we need to check memory usage
        while (getFreeHeap() < freeHeapLimit) {
          try{
            Thread.sleep(1000);
          }
          catch (InterruptedException e){
          }
        }

      }
      warcDocCountHistogram.update(warc.size());
      warcSizeHistogram.update(warcSize);
      warc.setBatchBytes(warcSize);

    } finally {
      ctx.stop();
    }
  }

  private long getFreeHeap(){
    return(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory())
        + Runtime.getRuntime().freeMemory();  	
  }
  public void restartForRestrictionsDomain() {
    if (isRunning()) {
      log.info("restartForRestrictionsDomain() : Restarting '{}'", getName());
      // By calling stop and then start we will 
      // block behind the running restrictions domain that currently has the lock.
      stop();
      start();
    } else {
      log.info("restartForRestrictionsDomain() : Not running... '{}'", getName());
    }
  }

  @Override
  public void autoStart() {
    if (runAtStart) {
      start();
    }
  }
}
