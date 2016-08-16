package bamboo.trove.full;

import javax.annotation.PostConstruct;

import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.WarcProgressManager;
import bamboo.trove.services.FilteringCoordinationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

@Service
public class FullReindexWarcManager extends BaseWarcDomainManager {
  private static final Logger log = LoggerFactory.getLogger(FullReindexWarcManager.class);

  private long warcMin = 127536;
  private long warcMax = 127773;

	@Autowired
	@Qualifier("solrDomainManager")
	private EndPointDomainManager solrManager;

	@Autowired
	private FilteringCoordinationService filteringService;

  private String bambooBaseUrl;
  private int maxFilterWorkers;
  private int maxTransformWorkers;
  private int maxIndexWorkers;

  private boolean running = false;
  private boolean stopping = false;
  private long warcsProcessed = 0;
  private long lastWarcId = warcMin - 1;

  @Required
  public void setBambooBaseUrl(String bambooBaseUrl) {
    this.bambooBaseUrl = bambooBaseUrl;
  }

  @Required
  public void setMaxFilterWorkers(int maxFilterWorkers) {
    this.maxFilterWorkers = maxFilterWorkers;
  }

  @Required
  public void setMaxTransformWorkers(int maxTransformWorkers) {
    this.maxTransformWorkers = maxTransformWorkers;
  }

  @Required
  public void setMaxIndexWorkers(int maxIndexWorkers) {
    this.maxIndexWorkers = maxIndexWorkers;
  }

  @PostConstruct
  public void init() {
		log.info("***** FullReindexWarcManager *****");
    BaseWarcDomainManager.startMe(bambooBaseUrl, maxFilterWorkers, maxTransformWorkers, maxIndexWorkers,
            solrManager, filteringService);
    log.info("Run at start      : {}", runAtStart);
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
    if (!running && !stopping)  {
      log.info("Starting...");
      running = true;
      loop();
    }
  }

  @Override
  public void stop() {
    if (running && !stopping)  {
      stopping = true;
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
    return "warc#" + lastWarcId;
  }

  private void loop() {
    while (!stopping) {
      doWork();
    }
    running = false;
    stopping = false;
  }

  private void doWork() {
    enqueueBatch(getNextWarc());
  }

  private WarcProgressManager getNextWarc() {
    if ((lastWarcId + 1) > warcMax) {
      stopping = true;
      return null;
    }
    WarcProgressManager result = getWarcFromBamboo(lastWarcId + 1);
    log.info("Warc #{} retrieved. {} docs", lastWarcId + 1, result.size());
    lastWarcId++;
    return result;
  }
}