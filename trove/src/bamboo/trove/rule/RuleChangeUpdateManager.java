package bamboo.trove.rule;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

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

import com.codahale.metrics.Timer;

import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import au.gov.nla.trove.indexer.api.BaseDomainManager;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.trove.common.DateRange;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.Rule;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.services.BambooRestrictionService;

public class RuleChangeUpdateManager extends BaseDomainManager implements Runnable, AcknowledgeWorker{
  private static final Logger log = LoggerFactory.getLogger(RuleChangeUpdateManager.class);
  
  private static final String[] SOLR_FIELDS = new String[]{"id", "url", "date"};
  private static final SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss'Z'");
	private static int NUMBER_OF_WORKERS = 5;
	private static int NUMBER_OF_DISTRIBUTORS = 3;

	@Autowired
	@Qualifier("solrDomainManager")
	private EndPointDomainManager solrManager;

  @Autowired
  private BambooRestrictionService restrictionsService;
  
	private String collection;
	private String zookeeperConfig = null;

	private WorkProcessor workProcessor;
	private WorkProcessor workDistributor;

  
  private List<Rule> changedRules;
  private List<Rule> dateRules;
  private Date lastProcessed = null;
	private String progress = null;
	private long updateCount = 0;
	private boolean running = false;
	private boolean stopping = false;
	private Date runStart = null;
	private CloudSolrClient client = null;

  @PostConstruct
  public void init() throws InterruptedException {
		log.info("***** RuleChangeUpdateManager *****");
		log.info("Solr zk path          : {}", zookeeperConfig);
		log.info("Collection            : {}", collection);
		log.info("Number of workers     : {}", NUMBER_OF_WORKERS);
		client = new CloudSolrClient(zookeeperConfig);
		client.setDefaultCollection(collection);
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		workProcessor = new WorkProcessor(NUMBER_OF_WORKERS);
		workDistributor = new WorkProcessor(NUMBER_OF_DISTRIBUTORS);
		lastProcessed = restrictionsService.getLastProcessed();
		if(restrictionsService.isRecovery()){
			log.info("Restart into Rule recovery mode.");
//			startProcessing();
		}
  }

	@Override
	public void run(){
		// check if we have a changed rule set.
		progress = "Checking for new Rules";
		Timer timer = getTimer(getName() + ".processRule");
		runStart = new Date();
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
		while(running){
			for(Rule r : deletedRules){
				Timer.Context context = timer.time();
				progress = "Processing " + changeCount++ + " deleted Rule : Rule<" + r.getId() + ">";
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
				progress = "Processing " + changeCount++ + " changed Rule : Rule<" + r.getId() + ">";
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
				progress = "Processing " + changeCount++ + " New Rule : Rule<" + r.getId() + ">";
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
				progress = "Processing " + changeCount++ + " date Rule : Rule<" + r.getId() + ">";
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
			progress = null;
			lastProcessed = restrictionsService.getLastProcessed();
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
		synchronized(documents){
			documents.add((String)doc.get("id").getValue());
		}
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
		String notLastIndexed = " AND lastIndexed:[* TO " + format(runStart) + "]";
		
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
			query = createQuery("url:"+url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(captureRangeChanged){
			SolrQuery query = createQuery(SolrEnum.URL + ":" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(retreivedRangeChanged){
			SolrQuery query = createQuery(SolrEnum.URL + ":" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(embargoChanged){
			SolrQuery query = createQuery(SolrEnum.RULE+":"+currentRule.getId() + notLastIndexed);
			processQuery(query);
			if(embargo > 0){
				Date embargoDate = new Date(runStart.getTime() - (embargo * 1000)); // change seconds to milli
				query = createQuery(SolrEnum.URL + ":"+url
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
			if (retrieved.isDateInRange(runStart)){
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
			String queryText = SolrEnum.URL + ":" + url + notLastIndexed;
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
		String queryText = SolrEnum.URL + ":" + url;
		if(retrieved != null){
			if (!retrieved.isDateInRange(runStart)){
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
  	q.setSort(SortClause.asc("id"));
  	q.setRows(1000);
  	return q;
	}
	
	private void processQuery(SolrQuery query) throws SolrServerException, IOException{
		log.debug("Query for rule : " + query.toString());
		Timer.Context context = getTimer(getName() + ".processQuery").time();

  	boolean more = true;
  	String cursor = CursorMarkParams.CURSOR_MARK_START;
  	while(more){
    	query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursor);
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
		// need to commit here so that we can ignore documents just processed 
		client.commit();
		context.stop();
	}
	
	private void distributeResponse(SolrDocumentList results){
		updateCount += results.size();
		RuleChangeUpdateManager manager = this;
		workDistributor.process(new Runnable(){
			
			@Override
			public void run(){
				// TODO Auto-generated method stub
		  	for(SolrDocument doc : results){
		  		String id = (String)doc.getFieldValue("id");
		  		String url = (String)doc.getFieldValue("url");
		  		Date capture = (Date)doc.getFieldValue("date");
//		  		log.debug("create worker URL:"+url+" id:"+id+" capture:"+ capture);
		  		RuleRecheckWorker worker = new RuleRecheckWorker(id, url, capture, manager, restrictionsService);
		  		workProcessor.process(worker);
		  	}				
			}
		});
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

//      if (timer != null) {
//        log.info("Cancelling batch management timer");
//        timer.cancel();
//      }
    }
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
	public static int getNumberOfWorkers(){
		return NUMBER_OF_WORKERS;
	}
	public static void setNumberOfWorkers(int numberOfWorkers){
		NUMBER_OF_WORKERS = numberOfWorkers;
	}
}
