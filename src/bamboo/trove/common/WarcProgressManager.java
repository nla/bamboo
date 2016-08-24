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

import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import bamboo.task.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarcProgressManager {
  private static final Logger log = LoggerFactory.getLogger(WarcProgressManager.class);
  private static final int POLL_INTERVAL_SECONDS = 1;
  private long warcId;

  // Queues for progress monitoring - single threaded access
  private Queue<IndexerDocument> filterProgress = new LinkedList<>();
  private int countFilterCompleted = 0;
  private Queue<IndexerDocument> transformProgress = new LinkedList<>();
  private int countTransformCompleted = 0;
  private Queue<IndexerDocument> indexProgress = new LinkedList<>();
  private int countIndexCompleted = 0;
  // Error queue
  protected BlockingQueue<IndexerDocument> errorQ = new ArrayBlockingQueue<>(5);
  protected int discardedErrors = 0;

  // Batch state
  private long timeStarted;
  private boolean filterComplete = false;
  private boolean transformComplete = false;
  private boolean indexComplete = false;
  private Timer timer;
  private int batchSize = 0;
  private long batchBytes = 0;
  private boolean loadingComplete = false;
  private boolean loadingFailed = false;

  // Sometimes we will be asked to hold a reference to a particular document
  private IndexerDocument trackedDocument = null;
  private long trackedOffset = -1;

  public WarcProgressManager(long warcId, long trackedOffset) {
    this.warcId = warcId;
    this.trackedOffset = trackedOffset;
    this.timeStarted = new Date().getTime();
    checkQueues();
  }

  public long getWarcId() {
    return warcId;
  }

  public long getTimeStarted() {
    return timeStarted;
  }

  public IndexerDocument add(Document document) {
    IndexerDocument doc = new IndexerDocument(warcId, document);
    enqueueDocument(doc);
    if (trackedDocument == null) {
      // Grab the first one through
      if (this.trackedOffset == -1) {
        trackedDocument = doc;
      }
      // We are after a specific one
      if (this.trackedOffset == document.getWarcOffset()) {
        trackedDocument = doc;
      }
    }
    return doc;
  }

  public IndexerDocument getTrackedDocument() {
    return trackedDocument;
  }

  public boolean isLoadingComplete() {
    return loadingComplete;
  }

  public void setLoadComplete() {
    loadingComplete = true;
  }

  public boolean isLoadingFailed() {
    return loadingFailed;
  }

  public void setLoadFailed() {
    loadingFailed = true;
  }

  private synchronized void enqueueDocument(IndexerDocument document) {
    filterProgress.add(document);
    transformProgress.add(document);
    indexProgress.add(document);
    batchSize++;
  }

  private synchronized void checkQueues() {
    // Filtering
    while (filterProgress.peek() != null && filterProgress.peek().filter.hasFinished()) {
      if (hasNoErrors(filterProgress.remove())) {
        countFilterCompleted++;
      }
    }

    // Transforming
    while (transformProgress.peek() != null && transformProgress.peek().transform.hasFinished()) {
      if (hasNoErrors(transformProgress.remove())) {
        countTransformCompleted++;
      }
    }

    // Indexing
    while (indexProgress.peek() != null && indexProgress.peek().index.hasFinished()) {
      if (hasNoErrors(indexProgress.remove())) {
        countIndexCompleted++;
      }
    }

    if (loadingComplete || loadingFailed) {
      // Would this occur?
      if (filterProgress.peek() == null && !filterProgress.isEmpty()) {
        log.error("Null object in queue! filterProgress: {}", this.warcId);
      }
      if (transformProgress.peek() == null && !transformProgress.isEmpty()) {
        log.error("Null object in queue! transformProgress: {}", this.warcId);
      }
      if (indexProgress.peek() == null && !indexProgress.isEmpty()) {
        log.error("Null object in queue! indexProgress: {}", this.warcId);
      }

      if (filterProgress.isEmpty()) {
        filterComplete = true;
      }
      if (transformProgress.isEmpty()) {
        transformComplete = true;
      }
      if (indexProgress.isEmpty()) {
        indexComplete = true;
      }
    }

    if (!(filterComplete && transformComplete && indexComplete)) {
      setTick();
    }
  }

  public boolean finished() {
    return filterComplete && transformComplete && indexComplete;
  }

  public boolean hasErrors() {
    return !errorQ.isEmpty() || discardedErrors > 0 || loadingFailed;
  }

  public boolean finishedWithoutError() {
    return finished() && !hasErrors();
  }

  private boolean hasNoErrors(IndexerDocument document) {
    if (document.isInError()) {
      for (IndexerDocument alreadyInError : errorQ) {
        if (alreadyInError.getDocId().equals(document.getDocId())) {
          // This is a duplicate, we already have it flagged as an error
          return false;
        }
      }

      if (errorQ.remainingCapacity() > 0) {
        log.debug("Warc #{}, adding doc {} to the error queue", warcId, document.getDocId());
        errorQ.offer(document);
      } else {
        // We have exceeded our capacity for this batch to recover without a restart after intervention.
        discardedErrors++;
      }
      return false;
    }
    return true;
  }

  private void setTick() {
    if (timer == null) {
      timer = new Timer();
    }
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        checkQueues();
      }
    }, POLL_INTERVAL_SECONDS * 1000);
  }

  public boolean isFilterComplete() {
    return filterComplete;
  }

  public boolean isTransformComplete() {
    return transformComplete;
  }

  public boolean isIndexComplete() {
    return indexComplete;
  }

  public int getCountFilterCompleted() {
    return countFilterCompleted;
  }

  public int getCountTransformCompleted() {
    return countTransformCompleted;
  }

  public int getCountIndexCompleted() {
    return countIndexCompleted;
  }

  public int size() {
    return batchSize;
  }

  public long getBatchBytes() {
    return batchBytes;
  }

  public void setBatchBytes(long batchBytes) {
    this.batchBytes = batchBytes;
  }
}