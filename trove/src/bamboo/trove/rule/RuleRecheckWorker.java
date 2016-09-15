package bamboo.trove.rule;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;

import bamboo.trove.common.Rule;
import bamboo.trove.services.BambooRestrictionService;

public class RuleRecheckWorker implements Runnable{

	private RuleChangeUpdateManager manager;
	private BambooRestrictionService service;
	private String id;
	private String url;
	private Date capture;
	
	public RuleRecheckWorker(String id, String url, Date capture, RuleChangeUpdateManager manager, BambooRestrictionService service){
		this.manager = manager;
		this.service = service;
		this.id = id;
		this.url = url;
		this.capture = capture;
	}
	@Override
	public void run(){
		SolrInputDocument update = processResultsRecheckRule();
		manager.update(update);
	}

	private SolrInputDocument processResultsRecheckRule(){ 
		Rule r = service.filterDocument(url, capture);
		SolrInputDocument update = new SolrInputDocument();
		update.addField("id", id);
		Map<String, Object> partialUpdate = new HashMap<>();
		partialUpdate.put("set", r.getId());
		update.addField("ruleId", partialUpdate);
		partialUpdate = new HashMap<>();
		partialUpdate.put("set", r.getPolicy().toString());
		update.addField("restricted", partialUpdate);
		partialUpdate = new HashMap<>();
		partialUpdate.put("set", new Date());
		update.addField("lastIndexed", partialUpdate);

//		System.out.println(update.toString());
		return update;
	}
}
