package bamboo.trove.workers;

import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.services.FilteringCoordinationService;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterWorker implements Runnable {
  private static Logger log = LoggerFactory.getLogger(FilterWorker.class);

  private FilteringCoordinationService filteringService;
  private Timer timer;

  public FilterWorker(FilteringCoordinationService filteringService, Timer timer) {
    this.filteringService = filteringService;
    this.timer = timer;
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
      log.error("Unexpected error during FilterWorker execution: ", ex);
      if (job != null) {
        job.setTransformError(ex);
      }
    }
    return true;
  }

  private IndexerDocument findJob() throws InterruptedException {
    IndexerDocument filterJob = BaseWarcDomainManager.getNextFilterJob();
    while (filterJob == null) {
      Thread.sleep(1000);
      filterJob = BaseWarcDomainManager.getNextFilterJob();
    }
    return filterJob;
  }

  private void doJob(IndexerDocument document) {
    document.filter.start(timer);
    filteringService.filterDocument(document);
    document.filter.finish();
  }
}