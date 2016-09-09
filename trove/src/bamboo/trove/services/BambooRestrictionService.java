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
package bamboo.trove.services;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import bamboo.app.Main;
import bamboo.task.Document;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.Rule;
import bamboo.trove.common.xml.RulePojo;
import bamboo.trove.common.xml.RulesPojo;
import bamboo.trove.db.RestrictionsDAO;
import bamboo.util.SurtFilter;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.xalan.lib.sql.QueryParameter;
import org.archive.util.SURT;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

/******
 * When requesting warc files from Bamboo there will be no awareness of restrictions carried with them.
 * 
 * To resolve this in an efficient fashion the indexer needs to maintain a long running representation of Bamboo's
 * restriction table that will filter warc contents on the way through the indexer.
 * 
 * Once a day (configurable) the service needs to request an update from Bamboo on new restriction information.
 * These changes can have two impacts:
 *  1) Newly applied restrictions need to find old documents in the index and flip them to restricted.
 *  2) Removed restrictions need to find old documents in the index (filtered by those which are currently restricted)
 *     and remove those restrictions.
 */
@Service
public class BambooRestrictionService {
  private static Logger log = LoggerFactory.getLogger(BambooRestrictionService.class);

  protected List<Rule> currentRules;
  protected List<Rule> newRules;
  private List<String> rawFilters; // <== this might need to become more complicated that a base string if a rule id is to be retained
  private FilterSegments segmentedFilters; // TODO: Allow vs deny?
  private List<SurtFilter> parsedFilters; // TODO: Time based embargoes. Can be time of content capture (ie. takedown) or time of indexing run (ie. embargo)

  private String bambooApiBaseUrl;
  private RestrictionsDAO dao;

	@Autowired
	private JdbiService database;

  @Required
  public void setBambooApiBaseUrl(String bambooApiBaseUrl) {
    this.bambooApiBaseUrl = bambooApiBaseUrl;
  }

  @PostConstruct
  public void init() {
    dao = database.getDao().restrictions();
    currentRules = dao.getCurrentRules();
    newRules = dao.getNewRules();
    rawFilters = new ArrayList<>();
    segmentedFilters = new FilterSegments();
    parsedFilters = new ArrayList<>();

    if (bambooApiBaseUrl == null) {
      throw new IllegalStateException("bambooApiBaseUrl has not been configured");
    }
    updateTick();
  }

  private void updateTick() {
  	List<Rule> rules = getRulesFromServer();
  	for(Rule r : rules){
  		String rest = "";
  		if(r.getEmbargoTime() > 0) rest += "embargo " + r.getEmbargoTime() +" ";
  		if(r.getCapturedRange() != null) rest += "capturt " + r.getCapturedRange().getStart() + " TO "+ r.getCapturedRange().getEnd() + " ";
  		if(r.getRetrievedRange() != null) rest += "retrieved "+ r.getRetrievedRange().getStart() + " TO "+ r.getRetrievedRange().getEnd()+ " ";
  		System.out.println(r.getId() + " : " + r.getPolicy() + " : " + r.getLastUpdated()
  			+ " : " + r.getSurt() + " : " + rest);
  	}

  	dao.addNewRuleSet(rules);
  	dao.makeNewRulesCurrent();
  	try{
			processChangedRules(rules);
		}
		catch (SolrServerException e){
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    // 1) Contact Bamboo
    // 2) Parse the response
    // 3) Process the response
    // 4) Flag follow up actions
    // 5) Schedule next update tick (? or we can do it all through quartz... a background thread here means we don't need to add quartz to the bamboo dependencies)
  }


  public Rule filterDocument(Document doc) {
  	return filterDocument(doc.getUrl(), doc.getDate());
  }
  
  public Rule filterDocument(String url, Date capture) {
    final Comparator<Rule> comp = (o1, o2) -> o1.compareTo(o2);
    Optional<Rule> r = currentRules.stream()
    		.filter(i -> i.matches(url, capture))
    			.max(comp);

    if(!r.isPresent()){
      throw new IllegalStateException("No matching rule found for : " + url);
    }
  	return r.get();
  }
  
public static void main(String[] args){
	BambooRestrictionService service = new BambooRestrictionService();  
	service.currentRules = new ArrayList<Rule>();
	Date now = new Date();
	service.currentRules.add(new Rule(1, DocumentStatus.REJECTED, now, 0, null, null, null, null, "(", false));
	service.currentRules.add(new Rule(2, DocumentStatus.ACCEPTED, now, 0, null, null, null, null, "(au,gov,", false));
	service.currentRules.add(new Rule(3, DocumentStatus.REJECTED, now, 0, null, null, null, null, "(au,gov,nla,", false));
	service.currentRules.add(new Rule(4, DocumentStatus.REJECTED, now, 0, null, null, null, null, "(au,gov,nla,trove,", false));
	service.currentRules.add(new Rule(5, DocumentStatus.REJECTED, now, 0, null, null, null, null, "(au,gov,nla,trove,)/home.html", false));
	Document doc = new Document();
	doc.setUrl("trove.nla.gov.au/home.html");
	Rule r = service.filterDocument(doc);
System.out.println(r.getId() + " : " + r.getPolicy());
doc.setUrl("trove.nla.gov.au/home.xml");
r = service.filterDocument(doc);
System.out.println(r.getId() + " : " + r.getPolicy());
doc.setUrl("trove.nla.gov.au/index.html");
r = service.filterDocument(doc);
System.out.println(r.getId() + " : " + r.getPolicy());
doc.setUrl("dlir.aec.gov.au/home.html");
r = service.filterDocument(doc);
System.out.println(r.getId() + " : " + r.getPolicy());

service.updateTick();
}
  // TODO: No consideration of embargo dates yet...
  // TODO: Polling background thread.
  // 1) Contact Bamboo and get current restriction list
  // 2) Diff current data against rawFilters.
  // 3) Rebuild parsedFilters from rawFilters
  // TODO: The rawFilters data should not be just in memory, it must be persisted somewhere to preserve state in the DB
  // TODO: Quartz scheduler job to run searches against the index after the updates completes
  // 1) Search for restricted content (??matching rules which were just removed) <= Requires indexing the rules used to restrict
  //       Maybe restrictedBy:{ruleId}
  // 2) Search for content in the index that matches (facet by segment?) a filter that was just added
  // TODO: Reindexing of content from above searches. Or atom update? We want to flip restricted flag, maybe ruleId and segments
  // TODO: If a search flags a large amount og content to be actioned it should halt and ask a staff member to intervene?

  public class FilterSegments extends HashMap<String, FilterSegments> {
    public void merge(FilterSegments newData) {
      if (newData == null || newData.isEmpty()) {
        return;
      }
      for (String key : newData.keySet()) {
        if (containsKey(key)) {
          get(key).merge(newData.get(key));
        } else {
          put(key, newData.get(key));
        }
      }
    }
  }
  
  private List<Rule> getRulesFromServer(){
  	List<Rule> rules;
  	
		try{
	    URL url = new URL(bambooApiBaseUrl);
	    URLConnection connection = (HttpURLConnection) url.openConnection();
	    InputStream in = new BufferedInputStream(connection.getInputStream());
	    rules = parseXML(in);
		}
		catch (IOException e){
			// TODO what should we do here 
			// 1. Stop with an error OR send an EMail and keep going with the old rules(no change.)
			throw new IllegalStateException("Error reading Rules from server." , e);
		}
    return rules;
//  	String xml = "<list>"
//  			+"  <rule>"
//  			+"    <id>1</id>"
//  			+"    <policy>ACCEPTED</policy>"
//  			+"    <surt>http://(</surt>"
//  			+"    <embargo/>"
//  			+"	<captureStart/>"
//  			+"	<captureEnd/>"
//  			+"	<retrievedStart/>"
//  			+"	<retrievedEnd/>"
//  			+"    <who></who>"
//  			+"    <privateComment></privateComment>"
//  			+"    <publicComment></publicComment>"
//  			+"    <exactMatch>false</exactMatch>"
//  			+"    <lastModified class=\"sql-timestamp\">2016-04-01 13:52:39.0</lastModified>"
//  			+"  </rule>"
//  			+"  <rule>"
//  			+"    <id>2</id>"
//  			+"    <policy>RESTRICTED_FOR_DELIVERY</policy>"
//  			+"    <surt>https://(tw,fred,</surt>"
//  			+"    <embargo>1000000</embargo>"
//  			+"	<captureStart/>"
//  			+"	<captureEnd/>"
//  			+"	<retrievedStart/>"
//  			+"	<retrievedEnd/>"
//  			+"    <who></who>"
//  			+"    <privateComment></privateComment>"
//  			+"    <publicComment></publicComment>"
//  			+"    <exactMatch>false</exactMatch>"
//  			+"    <lastModified class=\"sql-timestamp\">2014-09-11 18:14:54.0</lastModified>"
//  			+"  </rule>"
//  			+"  <rule>"
//  			+"    <id>26</id>"
//  			+"    <policy>RESTRICTED_FOR_BOTH</policy>"
//  			+"    <surt>https://(au,gov,aec,)/documents/data</surt>"
//  			+"    <embargo/>"
//  			+"    <captureStart class=\"sql-timestamp\">2015-10-02 00:11:56.0</captureStart>"
//  			+"    <captureEnd class=\"sql-timestamp\">2016-08-02 00:12:05.0</captureEnd>"
//  			+"    <retrievedStart/>"
//  			+"    <retrievedEnd/>"
//  			+"	<retrievedStart/>"
//  			+"	<retrievedEnd/>"
//  			+"    <who></who>"
//  			+"    <privateComment></privateComment>"
//  			+"    <publicComment></publicComment>"
//  			+"    <exactMatch>false</exactMatch>"
//  			+"    <lastModified class=\"sql-timestamp\">2016-08-09 14:12:10.0</lastModified>"
//  			+"  </rule>"
//  			+"</list>";
//  	ByteArrayInputStream is = new ByteArrayInputStream(xml.getBytes());
//  	return parseXML(is);
  }
  
  private List<Rule> parseXML(InputStream is){
  	List<Rule> rules = new ArrayList<Rule>();
  	JAXBContext context;
  	RulesPojo rs;
		try{
			context = JAXBContext.newInstance(RulesPojo.class);
	  	Unmarshaller unmarshaler = context.createUnmarshaller();
	  	rs = (RulesPojo)unmarshaler.unmarshal(is);
		}
		catch (JAXBException e){
			// TODO Should we stop here
			// Not able to parse the XML may be best to stop as the rules have changed(?) but contain errors 
			throw new IllegalStateException("Error parsing the returned XML.", e);
		}
		List<Integer> ids = new ArrayList<Integer>();
		for(RulePojo r : rs.getRules()){
			if(r.getSurt().startsWith("www"))continue; //TODO this should be an error invalid rule
			// Rules should have unique id's
			if(ids.contains(r.getId())){
				throw new IllegalStateException("Rule ID already exists : " + r.getId());
			}
			ids.add(r.getId());
			
			String p = r.getPolicy();
			if("allow".equals(p)){
				p = "ACCEPTED";
			}
			else if("block".equals(p)){
				p = "RESTRICTED_FOR_BOTH";
			}
			else if(p.isEmpty()){
				p = "RESTRICTED_FOR_BOTH";
			}
			DocumentStatus policy = DocumentStatus.valueOf(p);
			boolean matchExact = false;
			if("true".equalsIgnoreCase(r.getExactMatch())){
				matchExact = true;
			}
			rules.add(new Rule(r.getId(), policy, r.getLastModified(), r.getEmbargo(), r.getCaptureStart(), r.getCaptureEnd(), r.getViewStart(), r.getViewEnd(), r.getSurt(), matchExact));
		}
  	return rules;
  }
  
  /**
   * We are processing the rules that have changed.<p/>
   * 
   * We will have to check each rule that has changed(including deleted).<br/>
   * For each record in solr that was controlled by that rule we will have to check against all rules and update.
   * For all the records in solr that match the surt for the rule we need to check against all rules and update.  
   * @param changedRules
   * @throws SolrServerException 
   */
	private void processChangedRules(List<Rule> changedRules) throws SolrServerException{
  	CloudSolrServer client = new CloudSolrServer("localhost:2181/trove/0.5");
  	client.setDefaultCollection("pandora");
  	for(Rule rule : changedRules){
  		if(rule.getUrl().isEmpty())continue; // TODO this could be a full re-index ?
    	SolrQuery query = new SolrQuery("url:" + rule.getUrl());
    	query.setFields(new String[]{"fileNameHash", "url", "capture"});
    	query.setSort(SortClause.asc("fileNameHash"));
    	query.setRows(1000);
    	boolean more = true;
    	count = 0;
    	String cursor = CursorMarkParams.CURSOR_MARK_START;
    	while(more){
      	query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursor);
      	QueryResponse response = client.query(query);
      	SolrDocumentList results = response.getResults();
      	System.out.println(results.getNumFound());
      	String nextCursor = response.getNextCursorMark();
      	System.out.println("Cursor : " + nextCursor);
      	if(cursor.equals(nextCursor)){
      		more = false;
      	}
      	processResultsRecheckRule(results);
    		cursor = nextCursor;
    	}
  	}
  }
	
	int count = 0;
	private void processResultsRecheckRule(SolrDocumentList list){
  	for(SolrDocument doc : list){
  		String url = (String)doc.getFieldValue("url");
  		Date capture = (Date)doc.getFieldValue("capture");
  		String id = (String)doc.getFieldValue("fileNameHash");
  		System.out.println(count++ + " : " + id + " : " + url + " : " + capture);
  		Rule r = filterDocument(url, capture);
  		System.out.println(r.getId()+ " : "+r.getPolicy());
  		String update = "{add:{doc:{\"fileNameHash\":\"" + id
  				+"\", \"rule\":{set:"+r.getId()+"},\""
  				+ "restricted\":{set:\"" + r.getPolicy()+"\"}}}}";
  		try{
				URL solr = new URL("http://203.4.201.132:8981/solr/pandora/update/json");
        HttpURLConnection conn = (HttpURLConnection) solr.openConnection();
        conn.setRequestMethod("POST");
        conn.addRequestProperty("Content-Type", "application/json");
        conn.setFixedLengthStreamingMode(update.length());
        conn.setDoOutput(true);
        conn.getOutputStream().write(update.getBytes());
        System.out.println(conn.getResponseMessage());
			}
			catch (Exception e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
  	}
		
	}
}