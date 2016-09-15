package bamboo.trove.rule;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import au.gov.nla.trove.indexer.api.AcknowledgeWorker;
import au.gov.nla.trove.indexer.api.BaseDomainManager;
import au.gov.nla.trove.indexer.api.EndPointDomainManager;
import au.gov.nla.trove.indexer.api.WorkProcessor;
import bamboo.trove.common.DateRange;
import bamboo.trove.common.Rule;
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
	private String progress = "Rules last processed : ";
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
		if(restrictionsService.isRecovery()){
			log.info("Restart into Rule recovery mode.");
//			startProcessing();
		}
  }

	@Override
	public void run(){
		// check if we have a changed rule set.
		progress = "Checking for new Rules";
		runStart = new Date();
		changedRules = restrictionsService.getChangedRules();
		dateRules = restrictionsService.getDateRules();
		lastProcessed = restrictionsService.getLastProcessed();
		// remove any rules that have changed from the date list as we don't need to process twice.
		dateRules.removeAll(changedRules);
		log.debug("{} Rules have changed.", changedRules.size());

		int changeCount = 1;
		while(running){
			for(Rule r : changedRules){
				Rule current = restrictionsService.getRule(r.getId());
				progress = "Processing changed Rule " + changeCount++ + " : Rule<" + r.getId() + ">";
				try{
					findDocuments(current, r);
				}
				catch(IOException | SolrServerException e){
					setError("Error processing changed rule : " + r.getId(), e);
					stopProcessing();
				}
			}
			if(!changedRules.isEmpty()){
				// finished processing the changes so we will save the new rules
				restrictionsService.changeToNewRules(runStart);
			}
			for(Rule r : dateRules){
				progress = "Processing date Rule " + changeCount++ + " : Rule<" + r.getId() + ">";
				try{
					findDocuments(r, null);
				}
				catch(IOException | SolrServerException e){
					setError("Error processing changed rule : " + r.getId(), e);
					stopProcessing();
				}
			}
			progress = "Finished checking rules";
			stopProcessing();		
			if(stopping){
				running = false;
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
		log.debug("Find docs for rules {}", currentRule.getId());
//		if(rule.getUrl().isEmpty())continue; // TODO this could be a full re-index ?

		boolean urlChanged = false;
		boolean embargoChanged = false;
		boolean captureRangeChanged = false;
		boolean retreivedRangeChanged = false;
		
		// query part to stop records being processed more that once
		String notLastIndexed = " AND lastIndexed:[* TO " + format(runStart) + "]";
		
		String url = currentRule.getUrl();
		long embargo = currentRule.getEmbargo();
		DateRange captuer = currentRule.getCapturedRange(); 
		DateRange retrieved = currentRule.getRetrievedRange(); 
		if(newRule != null){
			log.debug("Rule {} has changed.", currentRule.getId());
			// use the new url if we have one
			if(url.equals(newRule.getUrl())){
				urlChanged = true;
			}
			if(embargo != newRule.getEmbargo()){
				embargoChanged = true;
			}
			if(captuer != null && captuer.equals(newRule.getCapturedRange())){
				captureRangeChanged = true; 
			}
			else if(newRule.getCapturedRange() != null && newRule.getCapturedRange().equals(captuer)){
				captureRangeChanged = true; 				
			}
			if(retrieved != null && retrieved.equals(newRule.getRetrievedRange())){
				retreivedRangeChanged = true; 
			}
			else if(newRule.getRetrievedRange() != null && newRule.getRetrievedRange().equals(retrieved)){
				retreivedRangeChanged = true; 
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
			SolrQuery query = createQuery("ruleId:"+currentRule.getId() + notLastIndexed);
			processQuery(query);
			query = createQuery("url:"+url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(captureRangeChanged){
			SolrQuery query = createQuery("url:" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(retreivedRangeChanged){
			SolrQuery query = createQuery("url:" + url + notLastIndexed);
			processQuery(query);
			return; // as we searched by url we should have tried all matching records.
		}
		if(embargoChanged){
			SolrQuery query = createQuery("ruleId:"+currentRule.getId() + notLastIndexed);
			processQuery(query);
			if(embargo > 0){
				Date embargoDate = new Date(runStart.getTime() - (embargo * 100)); // change seconds to milli
				query = createQuery("url:"+url
					+ " AND capture:[" + format(embargoDate) + " TO *]" 
					+ notLastIndexed);
				processQuery(query);
			}
			return;
		}
		
		// these are from no change to the rule so we are checking date coming into or going out of range
		boolean searchNeeded = false;
		if(embargo > 0){
			// only looking for records where the embargo has expired so search using rule
			SolrQuery query = createQuery("ruleId:" + currentRule.getId()	+ notLastIndexed);
			processQuery(query);
		}
		if(retrieved != null){
			// look for records set by the rule to see if still in date
			SolrQuery query = createQuery("ruleId:" + currentRule.getId()	+ notLastIndexed);
			processQuery(query);
			if (retrieved.isDateInRange(runStart)){
				// now is in range so we need to search by url
				searchNeeded = true;
			}
		}
		if(searchNeeded){
			String queryText = "url:" + url + notLastIndexed;
			SolrQuery query = createQuery(queryText);
			processQuery(query);
		}
	}
	
	private SolrQuery createQuery(String query){
		SolrQuery q = new SolrQuery(query);
  	q.setFields(SOLR_FIELDS);
  	q.setSort(SortClause.asc("id"));
  	q.setRows(1000);
  	return q;
	}
	
	private void processQuery(SolrQuery query) throws SolrServerException, IOException{
		log.debug("Query for rule : " + query.toString());
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
		
//  	for(SolrDocument doc : results){
//  		String id = (String)doc.getFieldValue("id");
//  		String url = (String)doc.getFieldValue("url");
//  		Date capture = (Date)doc.getFieldValue("date");
//  		log.debug("create worker URL:"+url+" id:"+id+" capture:"+ capture);
//  		RuleRecheckWorker worker = new RuleRecheckWorker(id, url, capture, this, restrictionsService);
//  		workProcessor.process(worker);
//  	}

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
		throw new IllegalArgumentException();
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
		return progress + (lastProcessed == null?"":lastProcessed.toString());
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
