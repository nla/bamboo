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
package bamboo.trove.services;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.annotations.VisibleForTesting;

import bamboo.task.Document;
import bamboo.trove.common.LastRun;
import bamboo.trove.common.cdx.CdxAccessControl;
import bamboo.trove.common.cdx.CdxAccessPolicy;
import bamboo.trove.common.cdx.CdxRule;
import bamboo.trove.common.cdx.RulesDiff;
import bamboo.trove.db.RestrictionsDAO;
import bamboo.trove.rule.RuleChangeUpdateManager;

/******
 * When requesting warc files from Bamboo there will be no awareness of restrictions carried with them.
 *
 * To resolve this in an efficient fashion the indexer needs to maintain a long running representation of OutbackCDX's
 * restriction table that will filter warc contents on the way through the indexer.
 *
 * Once a day (configurable) the service needs to request an update from OutbackCDX on new restriction information.
 * These changes can have two impacts:
 *  1) Newly applied restrictions need to find old documents in the index and flip them to restricted.
 *  2) Removed restrictions need to find old documents in the index (filtered by those which are currently restricted)
 *     and remove those restrictions.
 */
@Service
public class CdxRestrictionService {
  private static Logger log = LoggerFactory.getLogger(CdxRestrictionService.class);
  public static Date TODAY = new Date();

  private boolean recovery = false;
  private boolean inLockdown = false;
  private CdxAccessControl currentRules;
  private CdxAccessControl newRules;
  private CdxAccessControl rulesInUse;
  private LastRun updateRun;

  private String cdxBaseUrl;
  private RestrictionsDAO dao;

  @Autowired
  private JdbiService database;

  @Required
  public void setCdxBaseUrl(String cdxBaseUrl) {
    this.cdxBaseUrl = cdxBaseUrl;
    if (!cdxBaseUrl.endsWith("/")) {
      this.cdxBaseUrl += "/";
    }
  }

  @PostConstruct
  public void init() {
    dao = database.getDao().restrictions();
    currentRules = dao.getCurrentRules();
    newRules = dao.getNewRules();
    updateRun = dao.getLastRun();

    if (updateRun == null) {
      // No record of the last run... clean environment? Force an execution
      recovery = true;
      TODAY = new Date();
      if (newRules != null) {
        throw new IllegalStateException("New unprocessed rules were found in the database," +
                " but there is no recorded run progress. Looks like a data corruption.");
      }

    } else {
      // What date do we currently consider relevant to rules?
      if (updateRun.getDateCompleted() == null) {
        // We are in recovery and still processing nightly date updates
        recovery = true;
        TODAY = updateRun.getStarted();
        // Make sure we don't have data from the following step... that would be bad an indicate a logic bg of some sort
        if (newRules != null) {
          throw new IllegalStateException("New unprocessed rules were found in the database," +
                  " but our run progress says there should none be yet.");
        }
      } else {
        // We might be in recovery (check below) but we at least finished the date updates.
        TODAY = updateRun.getDateCompleted();
      }

      // We are in recovery but progressed to the second step at least
      if (updateRun.getAllCompleted() == null) {
        recovery = true;
        // IF we have a rules object at all then it must contain rules
        if (newRules != null && newRules.isEmpty()) {
          throw new IllegalStateException("There are no unprocessed rules in the database," +
                  " but our run progress says there should be some.");
        }
      }
    }

    log.info("Found {} current rules", currentRules == null ? 0 : currentRules.size());
    log.info("Found {} new rules", newRules == null ? 0 : newRules.size());
    if (updateRun == null) {
      log.info("Rules have never run before!");
    } else {
      log.info("Rules last run on '{}'. complete: '{}'", updateRun.getStarted(),
              updateRun.getAllCompleted() == null ? "INCOMPLETE" : updateRun.getAllCompleted());
    }

    if (newRules != null && !newRules.isEmpty()) {
      rulesInUse = newRules;
    } else {
      rulesInUse = currentRules;
    }
    if (cdxBaseUrl == null || "".equals(cdxBaseUrl)) {
      throw new IllegalStateException("Config 'cdxBaseUrl' has not been provided. This is required.");
    }
  }

  public RulesDiff checkForChangedRules() throws RulesOutOfDateException {
    // IF in recovery and we already have new rules we aren't going to talk to CDX
    if (recovery && newRules != null && !newRules.isEmpty()) {
      rulesInUse = newRules;
      RulesDiff diff;
      if (currentRules == null) {
        // First run in the environment
        diff = (new CdxAccessControl(new ArrayList<>())).checkForRulesChanges(newRules);
      } else {
        // Normal
        diff = currentRules.checkForRulesChanges(newRules);
      }
      // Either way the run might be half done
      Long progressId = dao.getLastRunProgress(updateRun.getId());
      diff.filterRules(progressId);
      return diff;
    }

    // We don't have the new rules yet... go get them
    CdxAccessControl freshRules;
    try {
      freshRules = getRulesFromServer();
    } catch (IOException ex) {
      log.error("Error talking to CDX Server to get rules update");
      throw new RulesOutOfDateException("", ex);
    }

    // Special case... first execution in any environment
    if (currentRules == null) {
      try {
        dao.addNewRuleSet(freshRules);

      } catch (JsonProcessingException e) {
        log.error("Rules format not as expected. Error in json processing whilst storing in the database:", e);
        throw new RulesOutOfDateException("Error processing nightly rules.", e);
      }
      newRules = freshRules;
      rulesInUse = newRules;
      log.info("Setting 'rulesInUse' to new value");
      // A funky way of generating a diff that will flag everything as 'new'
      return (new CdxAccessControl(new ArrayList<>())).checkForRulesChanges(newRules);
    }

    // Normal nightly update
    RulesDiff diff = currentRules.checkForRulesChanges(freshRules);
    if (diff.hasWorkLeft()) {
      // some rules have changed so we will save to the DB.
      log.info("Changed rules received.");
      try {
        dao.addNewRuleSet(freshRules);

      } catch (JsonProcessingException e) {
        log.error("Rules format not as expected. Error in json processing whilst storing in the database:", e);
        throw new RulesOutOfDateException("Error processing nightly rules.", e);
      }
      newRules = freshRules;
      currentRules = newRules;
      rulesInUse = newRules;
      return diff;
    }
    return null;
  }

  CdxRule filterDocument(Document doc) throws RulesOutOfDateException {
    if (inLockdown) {
      throw new RulesOutOfDateException("RestrictionService is in lockdown because of a rules failure.");
    }
    return filterDocument(doc.getDeliveryUrl(), doc.getDate());
  }

  public CdxRule filterDocument(String url, Date capture) {
    if (rulesInUse == null) {
      throw new IllegalStateException("Rules logic has failed to initialise/update correctly."
              + " 'rulesInUse' should never be null");
    }
    return rulesInUse.checkAccess(url, capture, TODAY);
  }

  public LastRun getLastProcessed() {
    return updateRun;
  }

  /**
   * Start a rule update process.
   * <p/>
   * This will check that the last run was finished and store a new run date in
   * the database or return the date of the last run.
   *
   * @return LastRun
   */
  public LastRun startProcess() {
    if (updateRun == null || updateRun.getAllCompleted() != null) {
      updateRun = dao.startNewRun();
    }
    return updateRun;
  }

  public List<CdxRule> getDateRules() {
    // We have been called at an inappropriate time
    if (updateRun.getDateCompleted() != null || currentRules == null) {
      return null;
    }
    List<CdxRule> dateBasedRules = currentRules.getDateBasedRules();
    // Make sure all processed documents are using the correct rule set
    rulesInUse = currentRules;
    // And filter out anything we have seen before on previous incomplete runs
    Long progressId = dao.getLastRunProgress(updateRun.getId());

    return dateBasedRules.stream()
            .filter(rule -> rule.getId() >= progressId)
            .collect(Collectors.toList());
  }

  public void lockDueToError() {
    // Not happy Jan!
    inLockdown = true;
  }

  public void storeWorkLog(RuleChangeUpdateManager.WorkLog workLog) {
    workLog.completed();
    dao.updateRunProgress(updateRun.getId(), workLog.getRuleId(), workLog.getSearches(), workLog.getDocuments(),
            workLog.getWritten(), workLog.getMsElapsed());
  }

  public void finishDateBasedRules() {
    if (updateRun.getDateCompleted() == null) {
      // We are only doing this in two steps to ensure we are using the same date as the one we stored in MySQL
      dao.finishDateBasedRun(updateRun.getId());
      updateRun = dao.getRunById(updateRun.getId());
    }
    TODAY = updateRun.getDateCompleted();
  }

  public void finishNightlyRun() {
    // Update run metadata
    if (updateRun.getAllCompleted() == null) {
      dao.finishNightyRun(updateRun.getId(), newRules);
      updateRun = dao.getRunById(updateRun.getId());
    }
    if(newRules != null){
    	// Handle ruleset swap
    	currentRules = dao.getCurrentRules();
    	rulesInUse = currentRules;
    	newRules = null;
    }

    // This will often be redundant, but just in case we were in recovery, reset the flag now
    recovery = false;
  }

  @VisibleForTesting
  void overwriteRulesForTesting(CdxAccessControl ruleSet, Date customDate) {
    log.warn("overwriteRulesForTesting() should only be called during unit tests");
    if (ruleSet != null) {
      currentRules = ruleSet;
      rulesInUse = ruleSet;
    }
    TODAY = customDate;
  }

  public class FilterSegments extends HashMap<String, FilterSegments> {
    @SuppressWarnings("unused")
    void merge(FilterSegments newData) {
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

  private CdxAccessControl getRulesFromServer() throws IOException {
    String policyUrl = cdxBaseUrl + "policies";
    String ruleUrl = cdxBaseUrl + "rules";

    List<CdxAccessPolicy> policies = getCdxObjectList(policyUrl, CdxAccessPolicy.class);
    List<CdxRule> rules = getCdxObjectList(ruleUrl, CdxRule.class);

    return new CdxAccessControl(policies, rules);
  }

  private <T> List<T> getCdxObjectList(String apiUrl, Class<T> clazz) throws IOException {
    HttpURLConnection connection = null;
    InputStream in = null;

    try {
      URL url = new URL(apiUrl);
      connection = (HttpURLConnection) url.openConnection();
      in = new BufferedInputStream(connection.getInputStream());

      JavaType type = dao.getJsonMapper().getTypeFactory().constructCollectionType(List.class, clazz);
      return dao.getJsonMapper().readValue(in, type);

    } finally {
      if (in != null) {
        in.close();
      }
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  public boolean isInRecovery() {
    return recovery;
  }

  public static class RulesOutOfDateException extends Exception {
    RulesOutOfDateException(String message) {
      super(message);
    }

    RulesOutOfDateException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
