package bamboo.trove.common;

import au.gov.nla.trove.indexer.api.AcknowledgableSolrInputDocument;
import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This class exists purely for diagnostic purposes. When testing performance improvements with multiple options for
 * talking to Solr, this class was added as a way of hotswapping in new end points without restarting. The indexer UI
 * was similarly altered to graph each endpoint independently.
 *
 * NOTE: This was not strongly considered in terms of thread safety for production use as it is a diagnostic class.
 */
public class EndPointRotator {
  private static final Logger log = LoggerFactory.getLogger(EndPointRotator.class);

  private static final int MINS = 5;
  private static final long ROTATION_TIMEOUT_MS = 1000 * 60 * MINS;
  private static Timer timer;

  static {
    timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        tick();
      }
    }, 0, ROTATION_TIMEOUT_MS);
  }

  private static final List<EndPointDomainManager> endPoints = new ArrayList<>();
  private static int index = 0;

  private static void tick() {
    if ((index + 1) >= endPoints.size()) {
      index = 0;
    } else {
      index++;
    }
  }

  public static void registerNewEndPoint(EndPointDomainManager manager) {
    endPoints.add(manager);
  }

  public static void add(SolrInputDocument doc, AcknowledgeWorker worker) {
    // Only make one call to get() in case the index changes whilst we are inside this method
    EndPointDomainManager endPoint = endPoints.get(index);
    if (endPoint.supportsAsyncAdd()) {
      endPoint.addAsync(new AsyncDocWrapper(doc, worker));
    } else {
      endPoint.add(doc, worker);
    }
  }

  static class AsyncDocWrapper implements AcknowledgableSolrInputDocument {
    private SolrInputDocument document;
    private AcknowledgeWorker acknowledgeWorker;

    AsyncDocWrapper(SolrInputDocument document, AcknowledgeWorker acknowledgeWorker) {
      this.document = document;
      this.acknowledgeWorker = acknowledgeWorker;
    }

    @Override
    public SolrInputDocument getSolrDocument() {
      return document;
    }

    @Override
    public void acknowledge(SolrInputDocument solrInputDocument) {
      acknowledgeWorker.acknowledge(solrInputDocument);
    }

    @Override
    public void errorProcessing(SolrInputDocument solrInputDocument, Throwable throwable) {
      acknowledgeWorker.errorProcessing(solrInputDocument, throwable);
    }
  }
}
