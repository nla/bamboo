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
package bamboo.trove.rule;

import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.DateRange;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.EndPointRotator;
import bamboo.trove.common.LastRun;
import bamboo.trove.common.Rule;
import bamboo.trove.common.SearchCategory;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.services.BambooRestrictionService;
import bamboo.trove.services.FilteringCoordinationService;
import com.codahale.metrics.Timer;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

@Service
public class RuleChangeUpdateManager extends BaseWarcDomainManager implements Runnable, AcknowledgeWorker{
  private static final Logger log = LoggerFactory.getLogger(RuleChangeUpdateManager.class);
  
  private static final String[] SOLR_FIELDS = new String[]{SolrEnum.ID.toString(), SolrEnum.URL.toString(), 
  				SolrEnum.DATE.toString(), SolrEnum.SEARCH_CATEGORY.toString(), SolrEnum.SITE.toString()};
  private static final SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss'Z'");
	private static int NUMBER_OF_WORKERS = 5;
	private static int NUMBER_OF_DISTRIBUTORS = 3;

  @Autowired
  private BambooRestrictionService restrictionsService;

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
//	private WorkProcessor workDistributor;

  
  private List<Rule> changedRules;
  private List<Rule> dateRules;
  private LastRun lastProcessed = null;
	private String progress = null;
	private long updateCount = 0;
	private boolean running = false;
	private boolean stopping = false;
	private boolean hasPassedLock = false;
	private LastRun runStart = null;
	private CloudSolrClient client = null;

  private boolean useAsyncSolrClient = false;
  private boolean indexFullText = false;

  public void setUseAsyncSolrClient(boolean useAsyncSolrClient) {
    this.useAsyncSolrClient = useAsyncSolrClient;
  }

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
    // so this startup pattern will look a little odd to align with that view of the work. This domain will configure
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
//		workDistributor = new WorkProcessor(NUMBER_OF_DISTRIBUTORS);
		lastProcessed = restrictionsService.getLastProcessed();
		boolean runNow = false;
		if(restrictionsService.isRecovery()){
			log.info("Restart into Rule recovery mode.");
			runNow = true;
		}
		else{
			long oneDayAgo = System.currentTimeMillis()-(24*60*60*1000);
			if(lastProcessed.getDate().getTime()<oneDayAgo){
				log.info("Restart into Rule processing mode as last check was more that a day ago.");
				runNow = true;
			}
			else{
				Date nextRun = nextRunDate();
				Schedule.nextRun(this, nextRun);
			}
		}
		if(runNow){
			startProcessing();
			// wait until the recovery process has had a chance to get the lock
			while(!hasPassedLock){
				try{
					Thread.sleep(1000);
				}
				catch (InterruptedException e){
					// ignore
				}
			}			
		}

		if (useAsyncSolrClient) {
      EndPointRotator.registerNewEndPoint(solrThroughputDomainManager);
    } else {
      EndPointRotator.registerNewEndPoint(solrManager);
    }

    // Never start this until all the end points are registered
		startMe(filteringService, indexFullText);
  }

	@Override
	public void run(){
		acquireDomainStartLock();
		hasPassedLock = true;
		try{
			for(BaseWarcDomainManager m : BaseWarcDomainManager.getDomainList()){
				m.restartForRestrictionsDomain();
			}
			// TODO: Disabled whilst we do distributed indexing
			//runInsideLock();
		}
		finally {
			releaseDomainStartLock();
		}
	}
	
	public void runInsideLock(){
		// check if we have a changed rule set.
		progress = "Checking for new Rules";
		Timer timer = getTimer(getName() + ".processRule");
		runStart = restrictionsService.startProcess();
		changedRules = new ArrayList<>();
		List<Rule> deletedRules = new ArrayList<>();
		List<Rule> newRules = new ArrayList<>();
		boolean changes = restrictionsService.checkForChangedRules();
		if(changes){
			changedRules = restrictionsService.getChangedRules();
			deletedRules = restrictionsService.getDeletedRules();
			newRules = restrictionsService.getNewRules();
		}
		dateRules = restrictionsService.getDateRules();
		// remove any rules that have changed from the date list as we don't need to process twice.
		removeById(dateRules, changedRules);
		log.debug("{} Rules have changed.", changedRules.size());

		int changeCount = 1;
		int totalChanges = deletedRules.size() + changedRules.size() + newRules.size() + dateRules.size();
		while(running){
			for(Rule r : deletedRules){
				Timer.Context context = timer.time();
				progress = "Processing (" + changeCount++ + " of " + totalChanges + "). Deleted Rule : Rule<" + r.getId() + ">";
				try{
					findDocumentsDeleteRule(r);
				}
				catch (SolrServerException | IOException e){
					setError("Error processing changed rule : " + r.getId(), e);
					stopProcessing();
				}
				finally {
					context.stop();
				}
			}
			for(Rule r : changedRules){
				Timer.Context context = timer.time();
				Rule newRule = restrictionsService.getRule(r.getId());
				progress = "Processing (" + changeCount++ + " of " + totalChanges + "). Changed Rule : Rule<" + r.getId() + ">";
				try{
					findDocuments(r, newRule);
				}
				catch(IOException | SolrServerException e){
					setError("Error processing changed rule : " + r.getId(), e);
					stopProcessing();
				}
				finally {
					context.stop();
				}
			}
			for(Rule r : newRules){
				Timer.Context context = timer.time();
				progress = "Processing (" + changeCount++ + " of " + totalChanges + "). New Rule : Rule<" + r.getId() + ">";
				try{
					findDocuments(null, r);
				}
				catch(IOException | SolrServerException e){
					setError("Error processing changed rule : " + r.getId(), e);
					stopProcessing();
				}
				finally {
					context.stop();
				}
			}
			for(Rule r : dateRules){
				Timer.Context context = timer.time();
				progress = "Processing (" + changeCount++ + " of " + totalChanges + "). Date Rule : Rule<" + r.getId() + ">";
				try{
					findDocuments(r, null);
				}
				catch(IOException | SolrServerException e){
					setError("Error processing changed rule : " + r.getId(), e);
					stopProcessing();
				}
				finally {
					context.stop();
				}
			}
			if(!changedRules.isEmpty() || !newRules.isEmpty() || !deletedRules.isEmpty()){
				// finished processing the changes so we will save the new rules
				restrictionsService.changeToNewRules(runStart);
			}
			else{
				restrictionsService.updateRunFinished(runStart);
			}
			progress = null;
			lastProcessed = restrictionsService.getLastProcessed();
			Date nextRun = nextRunDate();
			Schedule.nextRun(this, nextRun);
			stopProcessing();		
			if(stopping){
				running = false;
			}
		}
		running = false;
		stopping = false;
	}

	private void removeById(List<Rule> dateRules, List<Rule> changed){
		List<Integer> ids = new ArrayList<>();
		for(Rule r:changed){
			ids.add(r.getId());
		}
		Iterator<Rule> i = dateRules.iterator();
		while(i.hasNext()){
			Rule r = i.next();
			if(ids.contains(r.getId())){
				i.remove();
			}
		}
	}
	
	private List<String> documents = new ArrayList<>();
	@Override
	public void errorProcessing(SolrInputDocument doc, Throwable error){
//		documents.remove((Integer)doc.get("id").getValue());
		String id = (String)doc.get("id").getValue();
	
		this.setError("Error updateing document " + id, error);
		stopProcessing();
	}

	@Override
	public void acknowledge(SolrInputDocument doc){
		synchronized(documents){
			documents.remove((String)doc.get("id").getValue());
		}
	}
	
	protected void update(SolrInputDocument doc){
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
	 * @param currentRule
	 * @param newRule
	 * @throws IOException
	 * @throws SolrServerException
	 */
	private void findDocuments(Rule currentRule, Rule newRule) throws SolrServerException, IOException{
		log.debug("Find docs for rules {}", currentRule != null? currentRule.getId():newRule.getId());
//		if(rule.getUrl().isEmpty())continue; // TODO this could be a full re-index ?

		// query part to stop records being processed more that once
		String notLastIndexed = " AND " + SolrEnum.LAST_INDEXED + ":[* TO " + format(runStart.getDate()) + "]";
		
		if(currentRule == null){
			// this is a new rule search by url and possibly date
			findDocumentsNewRule(newRule, notLastIndexed);
			return;
		}
		if(newRule == null){
			// this is a current rule search date change processing
			findDocumentsDateRule(currentRule, notLastIndexed);
			return;
		}
		
		boolean urlChanged = false;
		boolean embargoChanged = false;
		boolean captureRangeChanged = false;
		boolean retreivedRangeChanged = false;
		boolean policyChanged = false;
		
		String url = currentRule.getUrl();
		long embargo = currentRule.getEmbargo();
		DateRange captuer = currentRule.getCapturedRange(); 
		DateRange retrieved = currentRule.getRetrievedRange(); 
		DocumentStatus policy = currentRule.getPolicy();
		if(newRule != null){
			log.debug("Rule {} has changed.", currentRule.getId());
			// use the new url if we have one
			if(!url.equals(newRule.getUrl())){
				urlChanged = true;
			}
			if(embargo != newRule.getEmbargo()){
				embargoChanged = true;
			}
			if(captuer != null && !captuer.equals(newRule.getCapturedRange())){
				captureRangeChanged = true; 
			}
			else if(newRule.getCapturedRange() != null && !newRule.getCapturedRange().equals(captuer)){
				captureRangeChanged = true; 				
			}
			if(retrieved != null && !retrieved.equals(newRule.getRetrievedRange())){
				retreivedRangeChanged = true; 
			}
			else if(newRule.getRetrievedRange() != null && !newRule.getRetrievedRange().equals(retrieved)){
				retreivedRangeChanged = true; 
			}
			if(policy != newRule.getPolicy()){
				policyChanged = true;
			}
			url = newRule.getUrl();
			if(embargo < newRule.getEmbargo()){
				// use the longest embargo time
				embargo = newRule.getEmbargo();
			}
			else{
				embargo = 0; // only search using the rule id as time got shorter
			}
			captuer = newRule.getCapturedRange();
			retrieved = newRule.getRetrievedRange();
			log.debug("Changed url:{} embargo:{} capture:{} retrieve:{}",urlChanged, embargoChanged, captureRangeChanged, retreivedRangeChanged);
		}
		if(url.trim().isEmpty()){
			log.info("URL is empty searching all records.");
			url = "*";
		}
		if(urlChanged){
			// url changed search old rule and new url
			SolrQuery query = createQuery(SolrEnum.RULE + ":"+currentRule.getId() + notLastIndexed);
			processQuery(query);
			query = createQuery(SolrEnum.URL_TOKENIZED + ":" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(captureRangeChanged){
			SolrQuery query = createQuery(SolrEnum.URL_TOKENIZED + ":" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(retreivedRangeChanged){
			SolrQuery query = createQuery(SolrEnum.URL_TOKENIZED + ":" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(embargoChanged){
			SolrQuery query = createQuery(SolrEnum.RULE+":"+currentRule.getId() + notLastIndexed);
			processQuery(query);
			if(embargo > 0){
				Date embargoDate = new Date(runStart.getDate().getTime() - (embargo * 1000)); // change seconds to milli
				query = createQuery(SolrEnum.URL_TOKENIZED + ":"+url
					+ " AND " + SolrEnum.DATE + ":[" + format(embargoDate) + " TO *]" 
					+ notLastIndexed);
				processQuery(query);
			}
			return;
		}
		if(policyChanged){
			SolrQuery query = createQuery(SolrEnum.RULE+":"+currentRule.getId() + notLastIndexed);
			processQuery(query);
			return; 			
		}
	}
	
	private void findDocumentsDateRule(Rule currentRule, String notLastIndexed) throws SolrServerException, IOException{
		// these are from no change to the rule so we are checking date coming into or going out of range
		boolean searchNeeded = false;
		String url = currentRule.getUrl();
		long embargo = currentRule.getEmbargo();
		DateRange captuer = currentRule.getCapturedRange(); 
		DateRange retrieved = currentRule.getRetrievedRange(); 
		if(embargo > 0){
			// only looking for records where the embargo has expired so search using rule
			SolrQuery query = createQuery(SolrEnum.RULE + ":" + currentRule.getId()	+ notLastIndexed);
			processQuery(query);
		}
		if(retrieved != null){
			if (retrieved.isDateInRange(runStart.getDate())){
				// now is in range so we need to search by url
				searchNeeded = true;
			}
			else{
				// look for records set by the rule to see if still in date
				SolrQuery query = createQuery(SolrEnum.RULE + ":" + currentRule.getId()	+ notLastIndexed);
				processQuery(query);
			}
		}
		if(searchNeeded){
			if(url.trim().isEmpty()){
				log.info("URL is empty searching all records.");
				url = "*";
			}
			String queryText = SolrEnum.URL_TOKENIZED + ":" + url + notLastIndexed;
			SolrQuery query = createQuery(queryText);
			processQuery(query);
		}
	}
	
	private void findDocumentsNewRule(Rule newRule, String notLastIndexed) throws SolrServerException, IOException{
		// these are from no change to the rule so we are checking date coming into or going out of range
		String url = newRule.getUrl();
		long embargo = newRule.getEmbargo();
		DateRange captuer = newRule.getCapturedRange(); 
		DateRange retrieved = newRule.getRetrievedRange(); 
		if(url.trim().isEmpty()){
			log.info("URL is empty searching all records.");
			url = "*";
		}
		String queryText = SolrEnum.URL_TOKENIZED + ":" + url;
		if(retrieved != null){
			if (!retrieved.isDateInRange(runStart.getDate())){
				// now is not in range so rule does not apply
				return;
			}
		}
		if(captuer != null){
			queryText += " AND " + SolrEnum.DATE + ":[" + format(captuer.getStart()) + " TO " + format(captuer.getEnd())+ "]";
		}
		if(embargo > 0){
			Date embargoDate = new Date(System.currentTimeMillis()-(embargo*1000));
			queryText += " AND " + SolrEnum.DATE + ":[" + format(embargoDate) + " TO *]";
		}
		queryText += " " + notLastIndexed;
		SolrQuery query = createQuery(queryText);
		processQuery(query);
	}

	private void findDocumentsDeleteRule(Rule deleteRule) throws SolrServerException, IOException{
		// this rule was delete so we have to recheck any records covered by this rule
		SolrQuery query = createQuery(SolrEnum.RULE + ":" + deleteRule.getId());
		processQuery(query);		
	}
	
	private SolrQuery createQuery(String query){
		SolrQuery q = new SolrQuery("*:*");
		q.setFilterQueries(query);
  	q.setFields(SOLR_FIELDS);
  	q.setSort(SortClause.asc(SolrEnum.ID.toString()));
  	q.setRows(1000);
  	return q;
	}
	
	private void processQuery(SolrQuery query) throws SolrServerException, IOException{
		log.debug("Query for rule : " + query.toString());
		Timer.Context context = getTimer(getName() + ".processQuery").time();
		// need to commit here so that we can ignore documents just processed 
		client.commit();

  	boolean more = true;
  	String cursor = CursorMarkParams.CURSOR_MARK_START;
  	while(more){
    	query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursor);
  		Timer.Context contextQuery = getTimer(getName() + ".query").time();

    	QueryResponse response = client.query(query);
    	SolrDocumentList results = response.getResults();
    	String nextCursor = response.getNextCursorMark();
    	if(cursor.equals(nextCursor)){
    		more = false;
    	}
    	distributeResponse(results);
//    	log.debug("work size {}", workProcessor.processingWaiting());
//    	log.debug("dist size {}", workDistributor.processingWaiting());
  		cursor = nextCursor;
  		contextQuery.stop();
  	}		
		// wait until batch finished
  	boolean empty = false;
		while(!empty){
  		log.debug("wait for batch : {}", documents.size());
			try{
				Thread.sleep(1000);
			}
			catch (InterruptedException e){
				// ignore
			}
	  	synchronized (documents){
				empty = documents.isEmpty();
    	}
  	}		
  	context.stop();
	}
	
	private void distributeResponse(SolrDocumentList results){
		updateCount += results.size();
		RuleChangeUpdateManager manager = this;
//		workDistributor.process(new Runnable(){
//			
//			@Override
//			public void run(){
		  	for(SolrDocument doc : results){
		  		String id = (String)doc.getFieldValue(SolrEnum.ID.toString());
		  		synchronized(documents){
		  			documents.add(id);
		  		}
		  		String url = (String)doc.getFieldValue(SolrEnum.URL.toString());
		  		Date capture = (Date)doc.getFieldValue(SolrEnum.DATE.toString());
		  		String site = (String)doc.getFieldValue(SolrEnum.SITE.toString());
		  		String sc = (String)doc.getFieldValue(SolrEnum.SEARCH_CATEGORY.toString());
		  		SearchCategory searchCategory = SearchCategory.fromValue(sc);
		  		if(searchCategory == null){
		  			log.warn("Invalid Search Category : " + sc + " for record id : " + id);
		  			searchCategory = SearchCategory.NONE;
		  		}
//		  		log.debug("create worker URL:"+url+" id:"+id+" capture:"+ capture);
		  		RuleRecheckWorker worker = 
		  				new RuleRecheckWorker(id, url, capture, site, searchCategory, manager, restrictionsService);
		  		workProcessor.process(worker);
		  	}				
//		}
//		});
	}
	
	@Override
	public boolean isRunning(){
		return running;
	}

	@Override
	public boolean isStopping(){
		return stopping;
	}

	@Override
	public void start(){
		startProcessing();
//		throw new IllegalArgumentException();
	}
	private void startProcessing(){
    if (!running && !stopping)  {
      log.info("Starting...");
      running = true;
      Thread me = new Thread(this);
      me.setName(getName());
      me.start();
    }
	}

	@Override
	public void stop(){
		throw new IllegalArgumentException();
	}
	
	public void stopProcessing(){
    if (running && !stopping)  {
      stopping = true;
      log.info("Stopping domain... ");
//      stopWorkers();
      log.info("All workers stopped!");
    }
	}

	/**
	 * Calculate the date time of the next run.
	 * 
	 * @return The time of the next run.
	 */
  private Date nextRunDate(){
		Calendar now = Calendar.getInstance();
		Calendar next = Calendar.getInstance();
		next.set(Calendar.HOUR_OF_DAY, scheduleTimeHour);
		next.set(Calendar.MINUTE, scheduleTimeMinute);
		if(next.before(now)){
			next.add(Calendar.DATE, 1);
		}
		return next.getTime();
  }
  
	private static String format(Date d){
		if(d == null){
			return "*";
		}
		synchronized(format){
			return format.format(d);
		}
	}
	
	@Override
	public String getName(){
		return "Change Rule Update Domain";
	}

	@Override
	public long getUpdateCount(){
		return updateCount;
	}

	@Override
	public String getLastIdProcessed(){
		if(progress != null){
			return progress;
		}
		return "Rules last processed : " + (lastProcessed == null?"":lastProcessed.toString());
	}

	public List<Rule> getChangedRules(){
		return changedRules;
	}
	public void setChangedRules(List<Rule> changedRules){
		this.changedRules = changedRules;
	}
	
	public String getCollection(){
		return collection;
	}
	public void setCollection(String collection){
		this.collection = collection;
	}
	public String getZookeeperConfig(){
		return zookeeperConfig;
	}
	public void setZookeeperConfig(String zookeeperConfig){
		this.zookeeperConfig = zookeeperConfig;
	}
	public int getScheduleTimeHour(){
		return scheduleTimeHour;
	}
	public void setScheduleTimeHour(int scheduleTimeHour){
		if(scheduleTimeHour < 0 || scheduleTimeHour > 23){
			throw new IllegalArgumentException("Hour must be between 0 and 23");
		}
		this.scheduleTimeHour = scheduleTimeHour;
	}
	public int getScheduleTimeMinute(){
		return scheduleTimeMinute;
	}
	public void setScheduleTimeMinute(int scheduleTimeMinute){
		if(scheduleTimeMinute < 0 || scheduleTimeMinute > 59){
			throw new IllegalArgumentException("Minute must be between 0 and 59");
		}
		this.scheduleTimeMinute = scheduleTimeMinute;
	}
	public static int getNumberOfWorkers(){
		return NUMBER_OF_WORKERS;
	}
	public static void setNumberOfWorkers(int numberOfWorkers){
		NUMBER_OF_WORKERS = numberOfWorkers;
	}

	static class Schedule implements Runnable{
		private RuleChangeUpdateManager manager;
		long nextRun;

		public static void nextRun(RuleChangeUpdateManager manager, Date nextRun){
			new Schedule(manager, nextRun);
		}
		/**
		 * Set a timer for the next run to check for new rules and re-check date rules.
		 */
		public Schedule(RuleChangeUpdateManager manager, Date nextRun){
			this.manager = manager;
			this.nextRun = nextRun.getTime();
			Thread t = new Thread(this);
			t.setName("Recheck Rules.");
			t.start();
			log.info("Set Scheduler to start Rule Check at " + nextRun);
		}
		
		@Override
		public void run(){
			while(nextRun > System.currentTimeMillis()){
				long sleepTime = nextRun - System.currentTimeMillis();
				if(sleepTime < 100){
					sleepTime = 100;
				}
				try{
					Thread.sleep(sleepTime);
				}
				catch (InterruptedException e){
					// ignore
				}
			}
			log.info("Scheduler start Rule Check.");
      // TODO - Renable this after the next run. Note, there have also been changes to DISCOVERABLE/DELIVERABLE
      // in the interim because we no longer add lucene segment data when 'false' is the desired value. Writes
      // originating elsewhere need to mirror this and reads need to search for 'NOT true' when 'false' is desired.
			//manager.startProcessing();
		}
	}
}
