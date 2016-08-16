package bamboo.trove.common;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import bamboo.task.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarcProgressManager {
  private static final Logger log = LoggerFactory.getLogger(WarcProgressManager.class);
  private static final int POLL_INTERVAL_SECONDS = 1;
  private long warcId;

  // Queues for work - multi threaded access
  private final Queue<IndexerDocument> filterQ = new ConcurrentLinkedQueue<>();
  private final Queue<IndexerDocument> transformQ = new ConcurrentLinkedQueue<>();
  private final Queue<IndexerDocument> indexQ = new ConcurrentLinkedQueue<>();
  // Queues for progress monitoring - single threaded access
  private Queue<IndexerDocument> filterProgress = new LinkedList<>();
  private Queue<IndexerDocument> transformProgress = new LinkedList<>();
  private Queue<IndexerDocument> indexProgress = new LinkedList<>();
  // Error queue
  private BlockingQueue<IndexerDocument> errorQ = new ArrayBlockingQueue<>(5);
  private int discardedErrors = 0;

  // Batch state
  private boolean filterComplete = false;
  private boolean transformComplete = false;
  private boolean indexComplete = false;
  private Timer timer;
  private int batchSize = 0;

  public WarcProgressManager(long warcId, List<Document> bambooDocuments) {
    this.warcId = warcId;
    if (bambooDocuments == null || bambooDocuments.isEmpty()) {
      throw new IllegalArgumentException("No documents provided!");
    }
    for (Document doc : bambooDocuments) {
      enqueueDocument(new IndexerDocument(warcId, doc));
    }
    checkQueues();
  }

  private void enqueueDocument(IndexerDocument document) {
    filterQ.add(document);
    transformQ.add(document);
    indexQ.add(document);
    filterProgress.add(document);
    transformProgress.add(document);
    indexProgress.add(document);
    batchSize++;
  }

  private void checkQueues() {
    // Filtering
    while (filterProgress.peek() != null && filterProgress.peek().filter.hasFinished()) {
      hasNoErrors(filterProgress.remove());
    }
    if (filterProgress.isEmpty()) {
      filterComplete = true;
    }

    // Transforming
    while (transformProgress.peek() != null && transformProgress.peek().transform.hasFinished()) {
      hasNoErrors(transformProgress.remove());
    }
    if (transformProgress.isEmpty()) {
      transformComplete = true;
    }

    // Indexing
    while (indexProgress.peek() != null && indexProgress.peek().index.hasFinished()) {
      hasNoErrors(indexProgress.remove());
    }
    if (indexProgress.isEmpty()) {
      indexComplete = true;
    }

    if (!filterComplete || !transformComplete || !indexComplete) {
      setTick();
    }
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

  public Queue<IndexerDocument> getFilterQ() {
    return filterQ;
  }

  public Queue<IndexerDocument> getTransformQ() {
    return transformQ;
  }

  public Queue<IndexerDocument> getIndexQ() {
    return indexQ;
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

  public int size() {
    return batchSize;
  }
}