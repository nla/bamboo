/*
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

import bamboo.trove.common.Rule;
import bamboo.trove.common.SearchCategory;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.services.BambooRestrictionService;
import bamboo.trove.workers.TransformWorker;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrInputDocument;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class RuleRecheckWorker implements Runnable{
//  private static final Logger log = LoggerFactory.getLogger(RuleRecheckWorker.class);

	private static final Map<String, Object> partialUpdateNull = new HashMap<>();
	private static final Map<String, Object> partialUpdateFalse = new HashMap<>();
	static{
		partialUpdateNull.put("set", null); // set to null to remove
		partialUpdateFalse.put("set", false);
	}

	private RuleChangeUpdateManager manager;
	private BambooRestrictionService service;
	private String id;
	private String url;
	private Date capture;
	private String site;
	private SearchCategory searchCategory;
	private float boost = 1.0f;
	
	public RuleRecheckWorker(String id, String url, Date capture, 
			String site, SearchCategory searchCategory, 
			RuleChangeUpdateManager manager, BambooRestrictionService service){
		this.manager = manager;
		this.service = service;
		this.id = id;
		this.url = url;
		this.capture = capture;
		this.site = site;
		this.searchCategory = searchCategory;
	}
	
	@Override
	public void run(){
		Timer.Context context = manager.getTimer(manager.getName() + ".worker").time();
		SolrInputDocument update = processResultsRecheckRule();
		manager.update(update);
		context.stop();			
	}
	
	private SolrInputDocument processResultsRecheckRule(){ 
		Rule r = service.filterDocument(url, capture);
		SolrInputDocument update = new SolrInputDocument();
		update.addField(SolrEnum.ID.toString(), id);
		Map<String, Object> partialUpdate = new HashMap<>();
		partialUpdate.put("set", r.getId());
		update.addField(SolrEnum.RULE.toString(), partialUpdate);
		switch (r.getPolicy()) {
			case RESTRICTED_FOR_BOTH:
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateFalse);				
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateFalse);
				modifyBoost(TransformWorker.MALUS_UNDELIVERABLE);
				break;
			case RESTRICTED_FOR_DELIVERY:
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateNull);				
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateFalse);				
	      // modifyBoost(TransformWorker.MALUS_UNDELIVERABLE);
				break;
			case RESTRICTED_FOR_DISCOVERY:
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateFalse);				
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateNull);				
				break;

			default:
				update.addField(SolrEnum.DELIVERABLE.toString(), partialUpdateNull);				
				update.addField(SolrEnum.DISCOVERABLE.toString(), partialUpdateNull);				
				break;
		}
		
		if(searchCategory == null){
			modifyBoost(TransformWorker.MALUS_SEARCH_CATEGORY);			
		}
		else{
  		switch (searchCategory) {
  			case NONE:
  			case DOCUMENT:
  			case PRESENTATION:
  			case SPREADSHEET:
  				modifyBoost(TransformWorker.MALUS_SEARCH_CATEGORY);
  				break;
  				
  			default:
  				break;
  		}
		}
		
		if(site != null){
    	if(site.endsWith(".gov.au")){
    		modifyBoost(TransformWorker.BONUS_GOV_SITE);
    	}
    	else if(site.endsWith(".edu.au")){
    		modifyBoost(TransformWorker.BONUS_EDU_SITE);
    	}
		}
		
		partialUpdate = new HashMap<>();
		partialUpdate.put("set", new Date());
		update.addField(SolrEnum.LAST_INDEXED.toString(), partialUpdate);
		update.setDocumentBoost(boost);

//		System.out.println(update.toString());
		return update;
	}
	
  public float modifyBoost(float modifier) {
    boost *= modifier;
    return boost;
  }
}
