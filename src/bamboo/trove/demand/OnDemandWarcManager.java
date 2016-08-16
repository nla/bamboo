package bamboo.trove.demand;

import java.util.Iterator;
import javax.annotation.PostConstruct;

import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.WarcProgressManager;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OnDemandWarcManager extends BaseWarcDomainManager {
  private static final Logger log = LoggerFactory.getLogger(OnDemandWarcManager.class);

  private boolean running = false;
  private long warcsProcessed = 0;
  private long lastWarcId = 0;

  @PostConstruct
  public void init() throws InterruptedException {
    BaseWarcDomainManager.waitUntilStarted();
		log.info("***** OnDemandWarcManager *****");
		log.info("Run at start       : {}", runAtStart);
  }

  // The UI will call here when it wants to start indexing a warc
	public String index(long warcId) throws Exception {
    return index(warcId, -1);
  }
  // Same as above, but flagging a particular offset as being of interest
	public String index(long warcId, long warcOffset) throws Exception {
    if (!running) {
      return "<error>Offline</error>";
    }

    log.info("Indexing on demand. Warc #{}", warcId);
    WarcProgressManager batch = getWarcFromBamboo(warcId);
    IndexerDocument responseDocument = batch.getFilterQ().peek();
    if (warcOffset < -1) {
      String target = warcId + "/" + warcOffset;
      Iterator<IndexerDocument> i = batch.getFilterQ().iterator();
      while (i.hasNext() && !target.equals(responseDocument.getDocId())) {
        responseDocument = i.next();
      }
      if (!target.equals(responseDocument.getDocId())) {
        return "<error>Warc Offest " + warcOffset + " could not be found in warc #" + warcId + "</error>";
      }
    }

    log.info("Warc #{} has {} documents. Starting filtering...", warcId, batch.size());
    enqueueBatch(batch);

    while (!batch.isFilterComplete()) {
      Thread.sleep(100);
    }
    log.info("Warc #{} has finished filtering. Starting tranform...", warcId);

    while (!batch.isTransformComplete()) {
      Thread.sleep(100);
    }
    log.info("Warc #{} has finished transform. Starting indexing...", warcId);

    while (!batch.isIndexComplete()) {
      Thread.sleep(100);
    }
    log.info("Warc #{} has finished indexing.", warcId);

    warcsProcessed++;
    lastWarcId = warcId;

    return ClientUtils.toXML(responseDocument.getSolrDocument());
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isStopping() {
    return false;
  }

  @Override
  public void start() {
    if (!running)  {
      log.info("Starting...");
      running = true;
    }
  }

  @Override
  public void stop() {
    if (running)  {
      running = false;
    }
  }

  @Override
  public String getName() {
    return "Web Archives On-Demand Indexing";
  }

  @Override
  public long getUpdateCount() {
    return warcsProcessed;
  }

  @Override
  public String getLastIdProcessed() {
    return "warc#" + lastWarcId;
  }
}