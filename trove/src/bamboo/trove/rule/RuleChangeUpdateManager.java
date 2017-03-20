/*
 * Copyright 2016-2017 National Library of Australia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bamboo.trove.rule;

import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.EndPointRotator;
import bamboo.trove.common.LastRun;
import bamboo.trove.common.SearchCategory;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.common.cdx.CdxDateRange;
import bamboo.trove.common.cdx.CdxRule;
import bamboo.trove.common.cdx.RulesDiff;
import bamboo.trove.services.CdxRestrictionService;
import bamboo.trove.services.FilteringCoordinationService;
import bamboo.util.Urls;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RuleChangeUpdateManager extends BaseWarcDomainManager implements Runnable, AcknowledgeWorker {
  private static final Logger log = LoggerFactory.getLogger(RuleChangeUpdateManager.class);

  private static final String[] SOLR_FIELDS = new String[] {SolrEnum.ID.toString(), SolrEnum.DISPLAY_URL.toString(),
          SolrEnum.DELIVERY_URL.toString(), SolrEnum.DATE.toString(), SolrEnum.SEARCH_CATEGORY.toString(),
          SolrEnum.SITE.toString(), SolrEnum.RULE.toString()};
  private static final SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss'Z'");
  private static int NUMBER_OF_WORKERS = 5;
  private static final ZoneId TZ = ZoneId.systemDefault();


  @Autowired
  private CdxRestrictionService restrictionsService;

  @Autowired
  @Qualifier("solrDomainManager")
  private EndPointDomainManager solrManager;

  @Autowired
  @Qualifier("solrThroughputDomainManager")
  private EndPointDomainManager solrThroughputDomainManager;

  @Autowired
  private FilteringCoordinationService filteringService;

  private String bambooBaseUrl;
  private int maxFilterWorkers;
  private int maxTransformWorkers;
  private int maxIndexWorkers;
  private int scheduleTimeHour;
  private int scheduleTimeMinute;

  private String collection;
  private String zookeeperConfig = null;

  private WorkProcessor workProcessor;

  private LastRun lastProcessed = null;
  private String progress = null;
  private long updateCount = 0;
  private boolean running = false;
  private boolean stopping = false;
  private boolean hasPassedLock = false;
  private CloudSolrClient client = null;

  private boolean useAsyncSolrClient = false;
  private boolean indexFullText = false;
  private boolean nightlyRunInProgress = false;
  private boolean earlyAbortNightlyRun = false;

  @SuppressWarnings("unused")
  public void setUseAsyncSolrClient(boolean useAsyncSolrClient) {
    this.useAsyncSolrClient = useAsyncSolrClient;
  }

  @SuppressWarnings("unused")
  public void setIndexFullText(boolean indexFullText) {
    this.indexFullText = indexFullText;
  }

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
    log.info("***** RuleChangeUpdateManager *****");
    // The core Trove indexer doesn't really match the model we have here were all of the domains share worker pools,
    // so this startup pattern will look a little odd to align with that view of the world. This domain will configure
    // and init (via statics) the base class all of the other domains extend. They will wait until we are done.
    BaseWarcDomainManager.setBambooApiBaseUrl(bambooBaseUrl);
    BaseWarcDomainManager.setWorkerCounts(maxFilterWorkers, maxTransformWorkers, maxIndexWorkers);
    // We must acquire the start lock before letting the other domains complete their init() methods.

    log.info("Solr zk path          : {}", zookeeperConfig);
    log.info("Collection            : {}", collection);
    log.info("Number of workers     : {}", NUMBER_OF_WORKERS);

    client = new CloudSolrClient(zookeeperConfig);
    client.setDefaultCollection(collection);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    workProcessor = new WorkProcessor(NUMBER_OF_WORKERS);
    lastProcessed = restrictionsService.getLastProcessed();

    // Find our initial run state
    boolean runNow = false;
    if (restrictionsService.isInRecovery()) {
      log.info("Restart into Rule recovery mode.");
      runNow = true;

    } else {
      long oneDayAgo = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
      if (lastProcessed != null && lastProcessed.getAllCompleted() != null
              && lastProcessed.getAllCompleted().getTime() < oneDayAgo) {
        log.info("Restart into Rule processing mode as last check was more that a day ago.");
        runNow = true;
      } else {
        Date nextRun = nextRunDate();
        Schedule.nextRun(this, nextRun);
      }
    }

    // Start running?
    if (runNow) {
      startProcessing();
      // wait until the recovery process has had a chance to get the lock
      while (!hasPassedLock) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // ignore. log at a very low level to avoid IDE objections about empty catch block
          log.trace("Sleep interrupted... sleeping again.", e);
        }
      }
    }

    // Typically this doesn't change, but the 'throughput' domain is experimental
    if (useAsyncSolrClient) {
      EndPointRotator.registerNewEndPoint(solrThroughputDomainManager);
    } else {
      EndPointRotator.registerNewEndPoint(solrManager);
    }

    // Never start this until all the end points are registered
    startMe(filteringService, indexFullText);
  }

  @Override
  public void run() {
    acquireDomainStartLock();
    hasPassedLock = true;
    try {
      BaseWarcDomainManager.getDomainList()
              .forEach(BaseWarcDomainManager::restartForRestrictionsDomain);
      // To reach this line we are now 'holding' the start lock
      // for all domains and are sure they have stopped.
      earlyAbortNightlyRun = false;
      nightlyRunInProgress = true;

      try {
        runInsideLock();

      } catch (CdxRestrictionService.RulesOutOfDateException e) {
        log.error("Rules update execution terminated due to error and rules are now out of date. " +
                "Halting all ingest until restriction rules are fixed.", e);
        restrictionsService.lockDueToError();
      }

      nightlyRunInProgress = false;

    } finally {
      releaseDomainStartLock();
    }
  }

  private void runInsideLock() throws CdxRestrictionService.RulesOutOfDateException {
    // 'Nightly' run starting
    progress = "Starting new update process";
    restrictionsService.startProcess();
    Timer timer = getTimer(getName() + ".processRule");

    // Process any date based rules
    List<CdxRule> dateRules = restrictionsService.getDateRules();
    if (dateRules != null && !dateRules.isEmpty()) {
      int changeCount = 1;
      int totalChanges = dateRules.size();
      Iterator<CdxRule> it = dateRules.iterator();
      while (running && !earlyAbortNightlyRun && it.hasNext()) {
        CdxRule rule = it.next();
        Timer.Context context = timer.time();
        progress = "Processing (" + changeCount++ + " of " + totalChanges + "). Date Rule : Rule<#" + rule.getId() + ">";
        try {
          WorkLog workLog = findDocuments(rule, null);
          restrictionsService.storeWorkLog(workLog);

        } catch (IOException | SolrServerException e) {
          setError("Error processing date rule : " + rule.getId(), e);
          stopProcessing();

        } finally {
          context.stop();
        }
      }
    }

    // Stop here and wait for any pending workers to finish processing stuff we just queue'd up
    // We are about to (maybe) change the live rule set and (definitely) update the 'TODAY' context
    waitUntilCaughtUp();

    // Check for early termination
    if (earlyAbortNightlyRun || !running) {
      log.warn("Aborting execution of nightly rules processing. Early terminated requested.");
      return;
    }

    // Update DB with progress through run. This will also update TODAY because
    // we are now up-to-date and about to begin the nightly rule changes.
    restrictionsService.finishDateBasedRules();

    // Go to the server for an update (maybe... we could be in recovery)
    RulesDiff diff = restrictionsService.checkForChangedRules();
    if (!diff.hasWorkLeft()) {
      // We are done. Awesome sauce
      restrictionsService.finishNightlyRun();
      running = false;
      stopping = false;
      return;
    }

    int changeCount = 1;
    int totalChanges = diff.size();
    while (running && !earlyAbortNightlyRun && diff.hasWorkLeft()) {
      RulesDiff.RulesWrapper work = diff.nextRule();

      Timer.Context context = timer.time();
      progress = "Processing (" + changeCount++ + " of " + totalChanges + "). Rule<#" + work.rule.getId()
              + ">, Reason: " + work.reason;
      try {
        WorkLog workLog = null;
        switch (work.reason) {
          case NEW:
            workLog = findDocuments(null, work.rule);
            break;
          case DELETED:
            workLog = findDocumentsDeleteRule(work.rule);
            break;
          case CHANGED:
            workLog = findDocuments(work.rule, work.newRule);
            break;
        }
        restrictionsService.storeWorkLog(workLog);

      } catch (IOException | SolrServerException e) {
        setError("Error processing rule : " + work.rule.getId(), e);
        stopProcessing();

      } finally {
        context.stop();
      }

      if (stopping) {
        running = false;
      }
    }

    // Check for early termination
    if (earlyAbortNightlyRun || !running) {
      log.warn("Aborting execution of nightly rules processing. Early terminated requested.");
      return;
    }

    // Stop here and wait for any pending workers to finish processing stuff we just queue'd up
    // We are about to (maybe) change the live rule set and (definitely) update the 'TODAY' context
    waitUntilCaughtUp();

    // Graceful completion
    restrictionsService.finishNightlyRun();
    running = false;
    stopping = false;
    progress = null;
    lastProcessed = restrictionsService.getLastProcessed();

    Schedule.nextRun(this, nextRunDate());
  }

  private final List<String> documents = new ArrayList<>();

  @Override
  public void errorProcessing(SolrInputDocument doc, Throwable error) {
//		documents.remove((Integer)doc.get("id").getValue());
    String id = (String) doc.get("id").getValue();

    this.setError("Error updateing document " + id, error);
    stopProcessing();
  }

  @Override
  public void acknowledge(SolrInputDocument doc) {
    synchronized (documents) {
      documents.remove(doc.get("id").getValue().toString());
    }
  }

  protected void update(SolrInputDocument doc) {
    solrManager.add(doc, this);
  }

  /**
   * Search solr for documents that are effected by this rule and send to be
   * rechecked.
   * <p/>
   * Depend on what has changed will decide on what and how many searched we do.<br/>
   * First we need to search for records that have been set by this rule(search
   * for the rule id) and then
   * <ul>
   * <li>Changed URL we will also need to search for records that match the
   * url(search for url).</li>
   * <li>Embargo changed(and gotten longer) we will also need to search in the
   * embargo period(search url and capture date).</li>
   * <li>Capture range changed(time extends earlier start or later end) we also need to search for capture date in the range(search url and capture date).</li>
   * <li>Retrieve date changed we also need to search if now is with in the range.</li>
   * </ul>
   *
   * @param currentRule The current rule in place
   * @param newRule The rule that will replace it
   * @throws IOException If network errors occur
   * @throws SolrServerException If errors occur inside the Solr servers
   */
  private WorkLog findDocuments(CdxRule currentRule, CdxRule newRule) throws SolrServerException, IOException {
    log.debug("Find docs for rule {}", currentRule != null ? currentRule.getId() : newRule.getId());

    // query part to stop records being processed more that once
    String notLastIndexed = SolrEnum.LAST_INDEXED + ":[* TO " + format(CdxRestrictionService.TODAY) + "]";

    if (currentRule == null) {
      // this is a new rule search by url and possibly date
      return findDocumentsNewRule(newRule, notLastIndexed);
    }
    if (newRule == null) {
      // this is a current rule search date change processing
      return findDocumentsDateRule(currentRule, notLastIndexed);
    }

    // Changed rules
    WorkLog workLog = new WorkLog(currentRule.getId());
    // Step 1.. find everything that is already impacted by this rule and reindex it
    SolrQuery query = createQuery(SolrEnum.RULE + ":" + currentRule.getId());
    query.addFilterQuery(notLastIndexed);
    processQuery(query, workLog);
    // Step 2.. find anything that would be covered by the new rule that hasn't already been re-indexed
    query = convertRuleToSearch(newRule, notLastIndexed);
    processQuery(query, workLog);
    // Job done
    return workLog;
  }

  private WorkLog findDocumentsDateRule(CdxRule dateBasedRule, String notLastIndexed) throws SolrServerException, IOException {
    WorkLog workLog = new WorkLog(dateBasedRule.getId());

    // these are from no change to the rule so we are checking date coming into or going out of range
    boolean urlSearchNeeded = false;

    // *******************
    // Access dates
    CdxDateRange accessDates = dateBasedRule.getAccessed();
    if (accessDates != null && accessDates.hasData()) {
      if (accessDates.contains(CdxRestrictionService.TODAY)) {
        // now is in range so we need to search by url
        urlSearchNeeded = true;

      } else {
        // Rule is no longer applicable. Look for records set by the rule to re-process them
        SolrQuery query = createQuery(SolrEnum.RULE + ":" + dateBasedRule.getId());
        query.addFilterQuery(notLastIndexed);
        processQuery(query, workLog);
        // Job done... this rule will no longer apply to anything in the index
        return workLog;
      }
    }

    // *******************
    // Embargoes
    if (dateBasedRule.getPeriod() != null) {
      // Any capture dates older than TODAY - embargo period should be checked for possible release
      Instant embargoStart = CdxRestrictionService.TODAY.toInstant().minus(dateBasedRule.getPeriod());
      SolrQuery query = createQuery(SolrEnum.RULE + ":" + dateBasedRule.getId());
      query.addFilterQuery(SolrEnum.DATE + ":[* TO " + format.format(Date.from(embargoStart)) + "]");
      query.addFilterQuery(notLastIndexed);
      processQuery(query, workLog);
      // Still need a URL search to find stuff coming in to restriction
      urlSearchNeeded = true;
    }

    // *******************
    // URL based search
    if (urlSearchNeeded) {
      SolrQuery query = convertRuleToSearch(dateBasedRule, notLastIndexed);
      processQuery(query, workLog);
    }
    return workLog;
  }

  @VisibleForTesting
  public SolrQuery convertRuleToSearch(CdxRule rule, String notLastIndexed) {
    // URL complexity first
    List<String> urlQueries = new ArrayList<>();
    for (String url : rule.getUrlPatterns()) {
      if (!url.trim().isEmpty()) {
        urlQueries.add(urlSearch(url));
      }
    }
    if (urlQueries.isEmpty()) {
      urlQueries.add("*:*");
    }
    SolrQuery query = createQuery("(" + StringUtils.join(urlQueries, ") OR (") + ")");

    // Filter out stuff we have touched already this run
    query.addFilterQuery(notLastIndexed);
    // Filter for Embargo
    if (rule.getPeriod() != null && !rule.getPeriod().isZero()) {
      // TODAY +/- embargo period
      ZonedDateTime today = ZonedDateTime.ofInstant(CdxRestrictionService.TODAY.toInstant(), TZ);
      Date embargoStart = Date.from(today.minus(rule.getPeriod()).toInstant());
      Date embargoEnd = Date.from(today.plus(rule.getPeriod()).toInstant());
      query.addFilterQuery(SolrEnum.DATE + ":[" + format.format(embargoStart)
              + " TO " + format.format(embargoEnd) + "]");
    }
    // Filter for Capture date
    if (rule.getCaptured() != null && rule.getCaptured().hasData()) {
      query.addFilterQuery(SolrEnum.DATE + ":[" + format.format(rule.getCaptured().start)
              + " TO " + format.format(rule.getCaptured().end) + "]");
    }
    // Worth noting we don't filter for access date because it is one of the
    // deciding data points in whether or not to run this query at all.
    return query;
  }

  private String urlSearch(String url) {
    return SolrEnum.URL_TOKENIZED + ":\"" + Urls.removeScheme(url) + "\"";
  }

  private WorkLog findDocumentsNewRule(CdxRule newRule, String notLastIndexed) throws SolrServerException, IOException {
    WorkLog workLog = new WorkLog(newRule.getId());

    // Check access dates
    CdxDateRange accessDates = newRule.getAccessed();
    if (accessDates != null && accessDates.hasData()) {
      if (!accessDates.contains(CdxRestrictionService.TODAY)) {
        // That was easy... the rule is not yet in effect
        return workLog;
      }
    }

    // Convert the rule to a search for new content
    SolrQuery query = convertRuleToSearch(newRule, notLastIndexed);
    processQuery(query, workLog);
    return workLog;
  }

  private WorkLog findDocumentsDeleteRule(CdxRule rule) throws SolrServerException, IOException {
    WorkLog workLog = new WorkLog(rule.getId());

    // this rule was deleted so we have to recheck any records currently covered by this rule
    SolrQuery query = createQuery(SolrEnum.RULE + ":" + rule.getId());
    processQuery(query, workLog);
    return workLog;
  }

  private SolrQuery createQuery(String query) {
    SolrQuery q = new SolrQuery("*:*");
    // TODO: Should we add a request handler to the solr cluster to get metrics
    // on the volume and/or performance of these searches in their own bucket?
    q.setFilterQueries(query);
    q.setFields(SOLR_FIELDS);
    q.setSort(SortClause.asc(SolrEnum.ID.toString()));
    q.setRows(1000);
    return q;
  }

  private void processQuery(SolrQuery query, WorkLog workLog) throws SolrServerException, IOException {
    log.debug("Query for rule : {}", query.toString());
    Timer.Context context = getTimer(getName() + ".processQuery").time();
    // need to commit here so that we can ignore documents just processed
    client.commit();

    boolean more = true;
    String cursor = CursorMarkParams.CURSOR_MARK_START;
    while (more) {
      query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursor);
      Timer.Context contextQuery = getTimer(getName() + ".query").time();

      QueryResponse response = client.query(query);
      workLog.ranSearch();
      SolrDocumentList results = response.getResults();
      log.debug("Found {} (of {} docs) in QT = {} ms", results.size(), results.getNumFound(), response.getQTime());
      String nextCursor = response.getNextCursorMark();
      if (cursor.equals(nextCursor)) {
        more = false;
      }
      distributeResponse(results, workLog);
      cursor = nextCursor;
      contextQuery.stop();
    }

    // We do this at a higher level too, so this would seem redundant. There is a trade-off. Allowing parallelism
    // between rules means rules can sometimes be re-processed redundantly. The higher level waitUntilCaughtUp() will
    // ensure we never process rules at the same time rules are being changed.
    // By doing a wait here as well however, we can collect accurate statistics about how much actual write activity we
    // are really generating by passing the workLog into the work pool.
    // When we have a better awareness of the typical work patterns it might be worth disabling this method call and
    // then stop collecting the metrics to improve throughput.
    waitUntilCaughtUp();
    context.stop();
  }

  private void waitUntilCaughtUp() {
    boolean empty = false;
    while (!empty) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
      synchronized (documents) {
        empty = documents.isEmpty();
      }
    }
  }

  private void distributeResponse(SolrDocumentList results, WorkLog workLog) {
    updateCount += results.size();
    RuleChangeUpdateManager manager = this;

    for (SolrDocument doc : results) {
      workLog.foundDocument();
      String id = (String) doc.getFieldValue(SolrEnum.ID.toString());
      synchronized (documents) {
        documents.add(id);
      }
      String deliveryUrl = (String) doc.getFieldValue(SolrEnum.DELIVERY_URL.toString());
      Date capture = (Date) doc.getFieldValue(SolrEnum.DATE.toString());
      String site = (String) doc.getFieldValue(SolrEnum.SITE.toString());
      String sc = (String) doc.getFieldValue(SolrEnum.SEARCH_CATEGORY.toString());
      SearchCategory searchCategory = SearchCategory.fromValue(sc);
      if (searchCategory == null) {
        log.warn("Invalid Search Category : " + sc + " for record id : " + id);
        searchCategory = SearchCategory.NONE;
      }

      RuleRecheckWorker worker =
              new RuleRecheckWorker(id, deliveryUrl, capture, site, searchCategory, manager, restrictionsService);
      // TODO... we do write activity on every document we receive... some way of checking whether
      // the write activity is required would be desirable
      workLog.wroteDocument();
      workProcessor.process(worker);
    }
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
    startProcessing();
  }

  private void startProcessing() {
    if (!running && !stopping) {
      log.info("Starting...");
      running = true;
      Thread me = new Thread(this);
      me.setName(getName());
      me.start();
    }
  }

  @Override
  public void stop() {
    throw new IllegalArgumentException();
  }

  private void stopProcessing() {
    if (running && !stopping) {
      stopping = true;
      log.info("Stopping domain... ");
    }
  }

  /**
   * Calculate the date time of the next run.
   *
   * @return The time of the next run.
   */
  private Date nextRunDate() {
    Calendar now = Calendar.getInstance();
    Calendar next = Calendar.getInstance();
    next.set(Calendar.HOUR_OF_DAY, scheduleTimeHour);
    next.set(Calendar.MINUTE, scheduleTimeMinute);
    if (next.before(now)) {
      next.add(Calendar.DATE, 1);
    }
    return next.getTime();
  }

  private static String format(Date d) {
    if (d == null) {
      return "*";
    }
    synchronized (format) {
      return format.format(d);
    }
  }

  @Override
  public String getName() {
    return "Change Rule Update Domain";
  }

  @Override
  public long getUpdateCount() {
    return updateCount;
  }

  @Override
  public String getLastIdProcessed() {
    if (progress != null) {
      return progress;
    }
    return "Rules last processed : " + (lastProcessed == null ? "" : lastProcessed.toString());
  }

  @SuppressWarnings("unused")
  public void setCollection(String collection) {
    this.collection = collection;
  }

  @SuppressWarnings("unused")
  public void setZookeeperConfig(String zookeeperConfig) {
    this.zookeeperConfig = zookeeperConfig;
  }

  @SuppressWarnings("unused")
  public void setScheduleTimeHour(int scheduleTimeHour) {
    if (scheduleTimeHour < 0 || scheduleTimeHour > 23) {
      throw new IllegalArgumentException("Hour must be between 0 and 23");
    }
    this.scheduleTimeHour = scheduleTimeHour;
  }

  @SuppressWarnings("unused")
  public void setScheduleTimeMinute(int scheduleTimeMinute) {
    if (scheduleTimeMinute < 0 || scheduleTimeMinute > 59) {
      throw new IllegalArgumentException("Minute must be between 0 and 59");
    }
    this.scheduleTimeMinute = scheduleTimeMinute;
  }

  @SuppressWarnings("unused")
  public static void setNumberOfWorkers(int numberOfWorkers) {
    NUMBER_OF_WORKERS = numberOfWorkers;
  }

  private static class Schedule implements Runnable {
    private RuleChangeUpdateManager manager;
    long nextRun;

    static void nextRun(RuleChangeUpdateManager manager, Date nextRun) {
      new Schedule(manager, nextRun);
    }

    /**
     * Set a timer for the next run to check for new rules and re-check date rules.
     */
    Schedule(RuleChangeUpdateManager manager, Date nextRun) {
      this.manager = manager;
      this.nextRun = nextRun.getTime();
      Thread t = new Thread(this);
      t.setName("Recheck Rules.");
      t.start();
      log.info("Set Scheduler to start Rule Check at " + nextRun);
    }

    @Override
    public void run() {
      while (nextRun > System.currentTimeMillis()) {
        long sleepTime = nextRun - System.currentTimeMillis();
        if (sleepTime < 100) {
          sleepTime = 100;
        }
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      log.info("Scheduler start Rule Check.");
      // TODO - There have been changes to DISCOVERABLE/DELIVERABLE because we no longer add lucene segment data
      // when 'false' is the desired value. Writes originating elsewhere need to mirror this and reads need to
      // search for 'NOT true' when 'false' is desired.
      manager.startProcessing();
    }
  }

  public static class WorkLog {
    private final long ruleId;
    private long searches = 0;
    private long documents = 0;
    private AtomicLong written = new AtomicLong(0);
    private long msElapsed = 0;
    private final long started;

    WorkLog(long ruleId) {
      this.started = System.currentTimeMillis();
      this.ruleId = ruleId;
    }

    void ranSearch() {
      searches++;
    }
    void foundDocument() {
      documents++;
    }
    void wroteDocument() {
      written.incrementAndGet();
    }
    public void completed() {
      msElapsed = System.currentTimeMillis() - started;
    }

    public long getRuleId() {
      return ruleId;
    }

    public long getSearches() {
      return searches;
    }

    public long getDocuments() {
      return documents;
    }

    public long getWritten() {
      return written.get();
    }

    public long getMsElapsed() {
      return msElapsed;
    }
  }
}
