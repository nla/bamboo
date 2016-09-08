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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;

import bamboo.task.Document;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.db.RestrictionsDAO;
import bamboo.util.SurtFilter;
import org.archive.util.SURT;
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
    rawFilters = new ArrayList<>();
    segmentedFilters = new FilterSegments();
    parsedFilters = new ArrayList<>();

    if (bambooApiBaseUrl == null) {
      throw new IllegalStateException("bambooApiBaseUrl has not been configured");
    }
    updateTick();
  }

  private void updateTick() {
    // 1) Contact Bamboo
    // 2) Parse the response
    // 3) Process the response
    // 4) Flag follow up actions
    // 5) Schedule next update tick (? or we can do it all through quartz... a background thread here means we don't need to add quartz to the bamboo dependencies)
  }

  public DocumentStatus filterDocument(Document doc) {
    String plainUrl = SURT.fromPlain(doc.getUrl());

    // TODO
    return DocumentStatus.ACCEPTED;
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
}