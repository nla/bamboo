package bamboo.trove.workers;

import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.IndexerDocument;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerWorker implements Runnable {
  private static Logger log = LoggerFactory.getLogger(IndexerWorker.class);

  private EndPointDomainManager solrManager;
  private Timer timer;
  private Timer dqTimer;

  public IndexerWorker(EndPointDomainManager solrManager, Timer timer) {
    this.solrManager = solrManager;
    this.timer = timer;
    // TODO: Remove dqTimer. it has no long term usefulness. It is just being used during development
    // to keep an eye on thread backlogs (or the possibility thereof) around the solr.add() method at
    // various levels of scale.
    dqTimer = SharedMetricRegistries.getOrCreate("serverMetrics").timer("dqTimer");
    if (dqTimer == null) {
      dqTimer = new Timer();
      SharedMetricRegistries.getOrCreate("serverMetrics").register("dqTimer", dqTimer);
    }
  }

  @Override
  public void run() {
    while (loop()) {}
  }

  public boolean loop() {
    IndexerDocument job = null;
    try {
      job = findJob();
      doJob(job);
      // Make sure we de-reference to ensure that an exception thrown inside findJob()
      // doesn't flag the error from last time through the loop
      job = null;

    } catch (InterruptedException ex) {
      log.error("Thread.Sleep() interrupted on worker that cannot find job. Resuming. MSG:'{}'", ex.getMessage());
      return true;

    } catch (Exception ex) {
      log.error("Unexpected error during IndexerWorker execution: ", ex);
      if (job != null) {
        job.setIndexError(ex);
      }
    }
    return true;
  }

  private IndexerDocument findJob() throws InterruptedException {
    IndexerDocument job = BaseWarcDomainManager.getNextIndexJob();
    while (job == null) {
      Thread.sleep(1000);
      job = BaseWarcDomainManager.getNextIndexJob();
    }
    return job;
  }

  private void doJob(IndexerDocument document) {
    Timer.Context ctx = dqTimer.time();
    try {
      document.index.start(timer);
      if (document.getTheshold().equals(ContentThreshold.NONE)) {
        document.index.finish();
        return;
      }
      // IndexerDocument implements AcknowledgeWorker so it will handle the timer.stop() and/or errors
      solrManager.add(document.getSolrDocument(), document);

    } finally {
      ctx.stop();
    }
  }
}