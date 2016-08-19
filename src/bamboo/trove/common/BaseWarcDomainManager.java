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
package bamboo.trove.common;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import au.gov.nla.trove.indexer.api.BaseDomainManager;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.task.Document;
import bamboo.trove.services.FilteringCoordinationService;
import bamboo.trove.workers.FilterWorker;
import bamboo.trove.workers.IndexerWorker;
import bamboo.trove.workers.TransformWorker;
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
import org.springframework.beans.factory.annotation.Required;

public abstract class BaseWarcDomainManager extends BaseDomainManager implements Runnable {
  private static Logger log = LoggerFactory.getLogger(BaseWarcDomainManager.class);

	protected boolean runAtStart;
  @Required
  public void setRunAtStart(boolean runAtStart) {
    this.runAtStart = runAtStart;
  }

  // Reading data from Bamboo
  private static Timer bambooReadTimer; 
  private static Timer bambooParseTimer; 
  private static Histogram warcDocCountHistogram;
  private static Histogram warcSizeHistogram;
  private static String bambooBaseUrl;
  private ObjectMapper objectMapper = new ObjectMapper();
  private JsonFactory jsonFactory = new JsonFactory();

  // Managing pool tasks
  private static Queue<IndexerDocument> filterQueue = new ConcurrentLinkedQueue<>();
  private static Queue<IndexerDocument> transformQueue = new ConcurrentLinkedQueue<>();
  private static Queue<IndexerDocument> indexQueue = new ConcurrentLinkedQueue<>();

  // Performing pool tasks
  private static WorkProcessor filterPool;
  private static WorkProcessor transformPool;
  private static WorkProcessor indexPool;

  // This could be more elegant... but rather that have static init() code here that runs itself we
  // want to get config from a particular domain. This is solely to be consistent with how the standard
  // Trove indexer works. So here we want all domains to park and wait until after the 'full' domain has
  // started the shared worker pools.
  private static boolean imStarted = false;
  protected static synchronized void startMe(String newBambooBaseUrl, int filters, int transformers, int indexers,
                                             EndPointDomainManager solr, FilteringCoordinationService filtering) {
    if (imStarted) {
      throw new IllegalStateException("You started me twice!");
    }
		log.info("Bamboo Base URL  : {}", newBambooBaseUrl);
		log.info("Metrics registry : {}", filtering.getMetricsRegistryName());
		log.info("# Filters        : {}", filters);
		log.info("# Transformers   : {}", transformers);
		log.info("# Indexers       : {}", indexers);

    bambooBaseUrl = newBambooBaseUrl;

    // Metrics are fun...
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(filtering.getMetricsRegistryName());
    bambooReadTimer = new Timer();
    metrics.register("bambooReadTimer", bambooReadTimer);
    bambooParseTimer = new Timer();
    metrics.register("bambooParseTimer", bambooParseTimer);
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
    filterPool = new WorkProcessor(filters);
    for (int i = 0; i < filters; i++) {
      filterPool.process(new FilterWorker(filtering, filterTimer));
    }

    // Transform workers
    transformPool = new WorkProcessor(transformers);
    for (int i = 0; i < transformers; i++) {
      transformPool.process(new TransformWorker(transformTimer));
    }

    // Indexing workers
    indexPool = new WorkProcessor(indexers);
    for (int i = 0; i < indexers; i++) {
      indexPool.process(new IndexerWorker(solr, indexTimer));
    }

    imStarted = true;
  }

  @VisibleForTesting
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
  public static void forTestSetBambooBaseUrl(String bambooBaseUrl) {
    if (imStarted) {
      throw new IllegalStateException("Unit tests only!!!");
    }
    BaseWarcDomainManager.bambooBaseUrl = bambooBaseUrl;
  }

  protected static void waitUntilStarted() throws InterruptedException {
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

  public static IndexerDocument getNextIndexJob(IndexerDocument lastJob) {
    return indexQueue.poll();
  }

  public WarcProgressManager getAndEnqueueWarc(long warcId) {
    return getAndEnqueueWarc(warcId, -1);
  }

  public WarcProgressManager getAndEnqueueWarc(long warcId, long trackedOffset) {
    Timer.Context ctx = bambooReadTimer.time();
    HttpURLConnection connection = null;
    WarcProgressManager warc = new WarcProgressManager(warcId, trackedOffset);

    try {
      URL url = new URL(bambooBaseUrl + warcId + "/text");
      connection = (HttpURLConnection) url.openConnection();
      InputStream in = new BufferedInputStream(connection.getInputStream());
      parseJson(warc, in);
      warc.setLoadComplete();
      return warc;

    } catch (IOException e) {
      log.error("Error talking to Bamboo: {}", e.getMessage());
      return null;

    } catch (Exception e) {
      log.error("Unknown error getting data from Bamboo: {}", e.getMessage());
      return null;

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
      ctx.stop();
    }
  }

  // Important to note that this stream based parsing works very simple because to the POJO's being parsed
  // are very simple. If the Document class became more complicated this method would have to be as well.
  private void parseJson(WarcProgressManager warc, InputStream in) throws IOException {
    Timer.Context ctx = bambooParseTimer.time();
    JsonParser json = jsonFactory.createParser(in);
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
      }
      warcDocCountHistogram.update(warc.size());
      warcSizeHistogram.update(warcSize);

    } finally {
      ctx.stop();
    }
  }

  @Override
  public void autoStart() {
    if (runAtStart) {
      start();
    }
  }
}