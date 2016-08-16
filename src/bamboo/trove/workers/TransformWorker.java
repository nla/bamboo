package bamboo.trove.workers;

import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.SolrEnum;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformWorker implements Runnable {
  private static Logger log = LoggerFactory.getLogger(TransformWorker.class);

  private Timer timer;

  public TransformWorker(Timer timer) {
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
      log.error("Unexpected error during TransformWorker execution: ", ex);
      if (job != null) {
        job.setTransformError(ex);
      }
    }
    return true;
  }

  private IndexerDocument findJob() throws InterruptedException {
    IndexerDocument job = BaseWarcDomainManager.getNextTransformJob();
    while (job == null) {
      Thread.sleep(1000);
      job = BaseWarcDomainManager.getNextTransformJob();
    }
    return job;
  }

  private void doJob(IndexerDocument document) {
    document.transform.start(timer);
    transform(document);
    document.transform.finish();
  }

  private void transform(IndexerDocument document) {
    // No indexing at all
    if (ContentThreshold.NONE.equals(document.getTheshold())) {
      return;
    }

    SolrInputDocument solr = new SolrInputDocument();
    solr.addField(SolrEnum.ID.toString(), document.getDocId());
    solr.addField(SolrEnum.URL.toString(), document.getBambooDocument().getUrl());
    solr.addField(SolrEnum.DATE.toString(), document.getBambooDocument().getDate());
    solr.addField(SolrEnum.TITLE.toString(), document.getBambooDocument().getTitle());
    solr.addField(SolrEnum.CONTENT_TYPE.toString(), document.getBambooDocument().getContentType());
    solr.addField(SolrEnum.SITE.toString(), document.getBambooDocument().getSite());
    solr.addField(SolrEnum.RESTRICTED.toString(), DocumentStatus.RESTRICTED.equals(document.getStatus()));

    if (ContentThreshold.METADATA_ONLY.equals(document.getTheshold())) {
      document.converted(solr);
      return;
    }

    if (ContentThreshold.DOCUMENT_START_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == First X words
    }
    if (ContentThreshold.UNIQUE_TERMS_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == Only unique terms
    }
    if (ContentThreshold.FULL_TEXT.equals(document.getTheshold())) {
      // TODO: Full text == Everything
    }

    document.converted(solr);
  }
}