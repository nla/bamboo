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
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

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
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import bamboo.task.Document;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.Rule;
import bamboo.trove.common.xml.RulePojo;
import bamboo.trove.common.xml.RulesPojo;
import bamboo.trove.db.RestrictionsDAO;
import bamboo.util.SurtFilter;

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

  private boolean recovery = false;
  protected List<Rule> currentRules;
  protected List<Rule> newRules;
  private Date lastRun;
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
    lastRun = dao.getLastRun();
    log.debug("Found {} current rules", currentRules.size());
    log.debug("Found {} new rules", newRules.size());
    log.debug("Rules last run on {}.", lastRun);
    if(!newRules.isEmpty()){
    	recovery = true;
    	log.warn("Server start up with new Rule set not finished. Recovery Reprocess new rule set.");
    }
    rawFilters = new ArrayList<>();
    segmentedFilters = new FilterSegments();
    parsedFilters = new ArrayList<>();

    if (bambooApiBaseUrl == null) {
      throw new IllegalStateException("bambooApiBaseUrl has not been configured");
    }
  }

  public boolean checkForChangedRules() {
  	if(recovery){
  		return true;
  	}
  	List<Rule> rules = getRulesFromServer();
  	for(Rule r : rules){
  		String rest = "";
  		if(r.getEmbargo() > 0) rest += "embargo " + r.getEmbargo() +" ";
  		if(r.getCapturedRange() != null) rest += "capturt " + r.getCapturedRange().getStart() + " TO "+ r.getCapturedRange().getEnd() + " ";
  		if(r.getRetrievedRange() != null) rest += "retrieved "+ r.getRetrievedRange().getStart() + " TO "+ r.getRetrievedRange().getEnd()+ " ";
  		System.out.println(r.getId() + " : " + r.getPolicy() + " : " + r.getLastUpdated()
  			+ " : " + r.getSurt() + " : " + rest);
  	}

  	if(haveRulesChanged(currentRules, rules)){
  		// some rules have changed so we will save to the DB.
  		log.info("Changed rules received.");
  		dao.addNewRuleSet(rules);
  		newRules = rules;
  		return true;
  	}
  	return false;
  }


  public Rule filterDocument(Document doc) {
  	return filterDocument(doc.getUrl(), doc.getDate());
  }
  
  public Rule filterDocument(String url, Date capture) {
  	List<Rule> rules = currentRules;
  	if(!newRules.isEmpty()){
  		rules = newRules;
  	}
    final Comparator<Rule> comp = (o1, o2) -> o1.compareTo(o2);
    Optional<Rule> r = rules.stream()
    		.filter(i -> i.matches(url, capture))
    			.max(comp);

    if(!r.isPresent()){
      throw new IllegalStateException("No matching rule found for : " + url);
    }
  	return r.get();
  }
  
  public Date getLastProcessed(){
  	return lastRun;
  }
  
  public Rule getRule(int id){
  	Rule r = null;
  	List<Rule> rules = currentRules;
  	if(!newRules.isEmpty()){
  		rules = newRules; // use the new rules
  	}
  	for(Rule i : rules){
  		if(i.getId() == id){
  			r = i;
  			break;
  		}
  	}
  	return r;
  }
  
	/**
	 * Get the list of rules that have changed.
	 * <p/>
	 * This is the list to process by re-checking the documents that may need
	 * there restrictions updated.<br/>
	 * This should only include rules that have changed(edited).
	 * 
	 * @return
	 */
  public List<Rule> getChangedRules(){
  	Map<Integer, Rule> nRuleMap = new HashMap<>();
  	for(Rule r : newRules){
  		nRuleMap.put(r.getId(), r);
  	}
  	List<Rule> changed = new ArrayList<>();
  	for(Rule cRule : currentRules){
  		Rule nRule = nRuleMap.remove(cRule.getId());
  		if(nRule != null){
  			if(!cRule.getLastUpdated().equals(nRule.getLastUpdated())){
  				changed.add(cRule);
  			}
  		}
  	}
  	return changed;
  }
  
  public List<Rule> getNewRules(){
  	Map<Integer, Rule> nRuleMap = new HashMap<>();
  	for(Rule r : newRules){
  		nRuleMap.put(r.getId(), r);
  	}
  	List<Rule> changed = new ArrayList<>();
  	for(Rule cRule : currentRules){
  		nRuleMap.remove(cRule.getId());
  	}
  	if(!nRuleMap.isEmpty()){
  		// new rule not in current rules so add to changed.
  		changed.addAll(nRuleMap.values());
  	}
  	return changed;  	
  }

  public boolean haveRulesChanged(List<Rule> current, List<Rule> next){
  	Map<Integer, Rule> nRuleMap = new HashMap<>();
  	for(Rule r : next){
  		nRuleMap.put(r.getId(), r);
  	}
  	for(Rule cRule : current){
  		Rule nRule = nRuleMap.remove(cRule.getId());
  		if(nRule != null){
  			if(!cRule.getLastUpdated().equals(nRule.getLastUpdated())){
  				return true; // changed
  			}
  		}
  		else{
  			return true; // deleted
  		}
  	}
  	if(!nRuleMap.isEmpty()){
  		return true; // new
  	}
  	return false;
  }
  public List<Rule> getDeletedRules(){
  	List<Rule> list = new ArrayList<>();
  	Map<Integer, Rule> map = new HashMap<>();
  	for(Rule r : currentRules){
  		map.put(r.getId(), r);
  	}
  	for(Rule r : newRules){
  		map.remove(r.getId());
  	}
  	list.addAll(map.values());
  	return list;
  }
	/**
	 * Get the list of rules that have date dependence.
	 * <p/>
	 * This is the list to process by re-checking the documents that may need
	 * there restrictions updated.<br/>
	 * This should only include rules that have dates in there rules(edited).
	 * 
	 * @return
	 */
  public List<Rule> getDateRules(){ 
  	List<Rule> list = new ArrayList<>();
  	List<Rule> current = currentRules;
  	// if we have a new set of rule we will use them as this will be applied after the changed rules.
  	if(!newRules.isEmpty()){
  		current = newRules;
  	}
  	for(Rule r : current){
  		if(r.getCapturedRange() != null){
  			list.add(r);
  		}
  		else if(r.getRetrievedRange() != null){
  			list.add(r);
  		}
  		else if(r.getEmbargo() > 0){
  			list.add(r);
  		}
  	}
  	return list;
  }
  
  public void changeToNewRules(Date lastRun){
  	dao.makeNewRulesCurrent(lastRun);
    currentRules = dao.getCurrentRules();
    newRules = dao.getNewRules(); // should now be empty
    lastRun = dao.getLastRun();
    recovery = false;
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
  	
//		try{
//	    URL url = new URL(bambooApiBaseUrl);
//	    URLConnection connection = (HttpURLConnection) url.openConnection();
//	    InputStream in = new BufferedInputStream(connection.getInputStream());
//	    rules = parseXML(in);
//		}
//		catch (IOException e){
//			// TODO what should we do here 
//			// 1. Stop with an error OR send an EMail and keep going with the old rules(no change.)
//			throw new IllegalStateException("Error reading Rules from server." , e);
//		}
//    return rules;
  	String xml = "<list>"
  			+"  <rule>"
  			+"    <id>1</id>"
  			+"    <policy>ACCEPTED</policy>"
  			+"    <surt>http://(</surt>"
  			+"    <embargo/>"
  			+"	<captureStart/>"
  			+"	<captureEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-04-01 13:52:39.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>3</id>"
  			+"    <policy>RESTRICTED_FOR_DISCOVERY</policy>"
  			+"    <surt>http://(uk,</surt>"
  			+"    <embargo/>"
  			+"	<captureStart/>"
  			+"	<captureEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-04-01 13:52:39.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>2</id>"
  			+"    <policy>RESTRICTED_FOR_DELIVERY</policy>"
  			+"    <surt>https://(tw,fred,</surt>"
  			+"    <embargo>15552000</embargo>"
  			+"	<captureStart/>"
  			+"	<captureEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2014-09-11 18:14:54.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>4</id>"
  			+"    <policy>RESTRICTED_FOR_BOTH</policy>"
  			+"    <surt>http://(nz,gov,bom,</surt>"
  			+"    <embargo/>"
  			+"	<captureStart/>"
  			+"	<captureEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-04-01 13:52:39.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>5</id>"
  			+"    <policy>ACCEPTED</policy>"
  			+"    <surt>http://(nz,gov,bom,)/dir/images/image.jpg</surt>"
  			+"    <embargo/>"
  			+"	<captureStart/>"
  			+"	<captureEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-04-01 13:52:39.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>6</id>"
  			+"    <policy>ACCEPTED</policy>"
  			+"    <surt>http://(nz,gov,bom,)/dir/images</surt>"
  			+"    <embargo/>"
  			+"	<captureStart class=\"sql-timestamp\">2016-08-21 00:00:00.0</captureStart>"
  			+"	<captureEnd class=\"sql-timestamp\">2016-08-21 23:59:59.0</captureEnd>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-04-02 13:52:39.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>7</id>"
  			+"    <policy>ACCEPTED</policy>"
  			+"    <surt>http://(nz,gov,bom,)/dir/images/updateing.js</surt>"
  			+"    <embargo/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd class=\"sql-timestamp\">2016-09-20 23:59:59.0</retrievedEnd>"
  			+"	<captureStart/>"
  			+"	<captureEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-04-01 13:52:39.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>26</id>"
  			+"    <policy>RESTRICTED_FOR_BOTH</policy>"
  			+"    <surt>https://(au,gov,aec,)/documents/data</surt>"
  			+"    <embargo/>"
  			+"    <captureStart class=\"sql-timestamp\">2015-03-02 00:11:56.0</captureStart>"
  			+"    <captureEnd class=\"sql-timestamp\">2016-08-02 00:12:05.0</captureEnd>"
  			+"    <retrievedStart/>"
  			+"    <retrievedEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-08-09 14:12:10.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>27</id>"
  			+"    <policy>RESTRICTED_FOR_BOTH</policy>"
  			+"    <surt>https://(au,fred,aec,)/documents/data</surt>"
  			+"    <embargo/>"
  			+"    <captureStart class=\"sql-timestamp\">2015-03-02 00:11:56.0</captureStart>"
  			+"    <captureEnd class=\"sql-timestamp\">2016-08-02 00:12:05.0</captureEnd>"
  			+"    <retrievedStart class=\"sql-timestamp\">2017-08-02 00:12:05.0</retrievedStart>"
  			+"    <retrievedEnd/>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-08-09 14:12:10.0</lastModified>"
  			+"  </rule>"
  			+"  <rule>"
  			+"    <id>28</id>"
  			+"    <policy>RESTRICTED_FOR_DELIVERY</policy>"
  			+"    <surt>https://(au,net,aec,)/documents/data</surt>"
  			+"    <embargo/>"
  			+"    <captureStart/>"
  			+"    <captureEnd/>"
  			+"    <retrievedStart class=\"sql-timestamp\">2015-08-02 00:12:05.0</retrievedStart>"
  			+"    <retrievedEnd class=\"sql-timestamp\">2017-08-02 00:12:05.0</retrievedEnd>"
  			+"	<retrievedStart/>"
  			+"	<retrievedEnd/>"
  			+"    <who></who>"
  			+"    <privateComment></privateComment>"
  			+"    <publicComment></publicComment>"
  			+"    <exactMatch>false</exactMatch>"
  			+"    <lastModified class=\"sql-timestamp\">2016-08-10 14:12:10.0</lastModified>"
  			+"  </rule>"
  			+"</list>";
  	ByteArrayInputStream is = new ByteArrayInputStream(xml.getBytes());
  	return parseXML(is);
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
  
	
	public boolean isRecovery(){
		return recovery;
	}
}