package bamboo.trove.rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

import au.gov.nla.trove.indexer.api.EndPointException;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.Rule;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.services.BambooRestrictionService;

public class RuleRecheckWorker implements Runnable{
  private static final Logger log = LoggerFactory.getLogger(RuleRecheckWorker.class);

  private static final int RETRY_TIMES = 5;
  private static final long[] RETRY_SLEEP = {10000, 30000, 60000, 60000, 60000, 60000}; 
	private static final Map<String, Object> partialUpdateTrue = new HashMap<>();
	private static final Map<String, Object> partialUpdateFalse = new HashMap<>();
	static{
		partialUpdateTrue.put("set", true);
		partialUpdateFalse.put("set", false);
	}
	
	private RuleChangeUpdateManager manager;
	private BambooRestrictionService service;
	private Record record;
	private int retryCount = 0;
	
	public RuleRecheckWorker(Record record, RuleChangeUpdateManager manager, BambooRestrictionService service){
		this.manager = manager;
		this.service = service;
		this.record = record;
	}
	@Override
	public void run(){
		while(true){
  		Timer.Context context = manager.getTimer(manager.getName() + ".worker").time();
  		try{
  			RuleAcknowledgeWorker ack = new RuleAcknowledgeWorker(record.getId(), manager);
    		List<SolrInputDocument> updates = processResultsRecheckRule();
  			if(record.isDeleteNeeded()){
  				//remove records not needed
  				String id = record.getId();
  				for(int i=1;i<=record.getDeleteCount();i++){
  					ack.addSendCount();
  					manager.delete(id+"_"+i, ack);
  				}
  			}
    		for(SolrInputDocument update: updates){
					ack.addSendCount();
    			manager.update(update, ack);
    		}
    		ack.allSent();
    		break;
  		}
  		catch (SolrServerException | IOException e){
  			if(retryCount >= RETRY_TIMES){
  				log.error("Error getting full record from solr. ID:"+record.getId(), e);
  				manager.errorProcessing(record, e);
  				break;
  			}
  		}
			catch (EndPointException e){
  			if(retryCount >= RETRY_TIMES){
  				log.error("Error deleting record from solr. ID:"+record.getId(), e);
  				manager.errorProcessing(record, e);
  				break;
  			}
			}
  		finally {
  			context.stop();			
  		}
  		try{
				Thread.sleep(RETRY_SLEEP[retryCount++]);
			}
			catch (InterruptedException e){
				// ignore
			}
		}
	}

	private List<SolrInputDocument> processResultsRecheckRule() throws SolrServerException, IOException{
		Map<Rule, List<Date>> rules = service.filterDocument(record.getUrl(), record.getDate());

		if(rules.size() < record.getRuleIds().size()){
			record.setDeleteCount(record.getRuleIds().size());
		}
		
		if(rules.size() == 1){
				return createUpdateMainDoc(rules.keySet());				
		}
		return createSplitUpdateDoc(rules);
	}
	
	private List<SolrInputDocument> createSplitUpdateDoc(Map<Rule,List<Date>> rules) throws SolrServerException, IOException{
		// we have to have the full solr record as we have to split
		record = manager.getRecord(record.getId());

		// update main record with all matched rules and disable for searching and viewing
		List<SolrInputDocument> docs = createUpdateMainDoc(rules.keySet());

		// add new documents for each rule
		int docCount = 1;
		for(Rule r : rules.keySet()){
      docs.add(createSolrDoc(record.getId() + "_"+ docCount++, r, rules.get(r)));
		}
		return docs;
	}

	private SolrInputDocument createSolrDoc(String id, Rule r, List<Date> dates){
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(SolrEnum.ID.toString(), id);
    doc.addField(SolrEnum.URL.toString(), record.getUrl());
    doc.addField(SolrEnum.DATE.toString(), dates);
    doc.addField(SolrEnum.TITLE.toString(), record.getTitle());
    doc.addField(SolrEnum.CONTENT_TYPE.toString(), record.getContentType());
    doc.addField(SolrEnum.SITE.toString(), record.getSite());
    doc.addField("byRuleId", r.getId());
    doc.addField(SolrEnum.TEXT.toString(), record.getText());
    doc.addField(SolrEnum.TEXT_ERROR.toString(), record.getTextError());
		doc.addField(SolrEnum.LAST_INDEXED.toString(), new Date());

		switch (r.getPolicy()) {
			case RESTRICTED_FOR_BOTH:
				doc.addField(SolrEnum.DELIVERABLE.toString(), false);				
				doc.addField(SolrEnum.DISCOVERABLE.toString(), false);				
				break;
			case RESTRICTED_FOR_DELIVERY:
				doc.addField(SolrEnum.DISCOVERABLE.toString(), true);				
				doc.addField(SolrEnum.DELIVERABLE.toString(), false);				
				break;
			case RESTRICTED_FOR_DISCOVERY:
				doc.addField(SolrEnum.DELIVERABLE.toString(), false);				
				doc.addField(SolrEnum.DISCOVERABLE.toString(), true);				
				break;

			default:
				doc.addField(SolrEnum.DELIVERABLE.toString(), true);				
				doc.addField(SolrEnum.DISCOVERABLE.toString(), true);				
				break;
		}		
		return doc;
	}
	
	private List<SolrInputDocument> createUpdateMainDoc(Set<Rule> rules){
		SolrInputDocument update = new SolrInputDocument();
		update.addField(SolrEnum.ID.toString(), record.getId());
		Map<String, Object> partialUpdate = new HashMap<>();
//		String ruleIds = ""; 
		int  ruleIdCount = 0;
		List<Integer>  ruleIds = new ArrayList<>();
		for(Rule r : rules){
//			if(ruleIdCount++ > 0){
//				ruleIds += ", ";
//			}
//			ruleIds += r.getId();
			ruleIds.add(r.getId());
		}
//		if(ruleIdCount > 1){
//			ruleIds = "[" + ruleIds + "]";
//		}
		partialUpdate.put("set", ruleIds);
		update.addField(SolrEnum.RULE.toString(), partialUpdate);
		DocumentStatus policy = DocumentStatus.RESTRICTED_FOR_BOTH;
		if(rules.size() == 1){
			policy = rules.iterator().next().getPolicy();
		}
		switch (policy) {
			case RESTRICTED_FOR_BOTH:
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateFalse);				
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateFalse);				
				break;
			case RESTRICTED_FOR_DELIVERY:
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateTrue);				
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateFalse);				
				break;
			case RESTRICTED_FOR_DISCOVERY:
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateFalse);				
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateTrue);				
				break;

			default:
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateTrue);				
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateTrue);				
				break;
		}
		partialUpdate = new HashMap<>();
		partialUpdate.put("set", new Date());
		update.addField(SolrEnum.LAST_INDEXED.toString(), partialUpdate);

//		System.out.println(update.toString());
		List<SolrInputDocument> updates = new ArrayList<>();
		updates.add(update);
		return updates;
	}
}
