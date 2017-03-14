/*
 * Copyright 2016-2017 National Library of Australia
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
package bamboo.trove.db;

import bamboo.trove.common.LastRun;
import bamboo.trove.common.cdx.CdxAccessControl;
import bamboo.trove.common.cdx.CdxRule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.springframework.http.converter.json.Jackson2ObjectMapperFactoryBean;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

@RegisterMapper({RestrictionsDAO.CdxRuleMapper.class, RestrictionsDAO.LastRunMapper.class})
public abstract class RestrictionsDAO implements Transactional<RestrictionsDAO> {
  private static final String TABLE_RULESET = "index_persistence_web_archives_restrictions";
  private static final String TABLE_RULES   = TABLE_RULESET + "_rules";
  private static final String TABLE_RUN     = TABLE_RULESET + "_last_run";

  private static final String COLUMN_STAT_RULES = "workRules";
  private static final String COLUMN_STAT_SEARCHES = "workSearches";
  private static final String COLUMN_STAT_DOCS = "workDocuments";
  private static final String COLUMN_STAT_WRITTEN = "workWritten";
  private static final String COLUMN_STAT_ELAPSED = "workMsElapsed";

  // Save including this below
  private static final String COLUMN_STATS_ALL = COLUMN_STAT_RULES + ", " + COLUMN_STAT_SEARCHES + ", "
          + COLUMN_STAT_DOCS + ", " + COLUMN_STAT_WRITTEN + ", " + COLUMN_STAT_ELAPSED;
  private static final String COLUMN_STATS_UPDATE_SQL =
          COLUMN_STAT_RULES      + " = " + COLUMN_STAT_RULES    + " + 1, "
          + COLUMN_STAT_SEARCHES + " = " + COLUMN_STAT_SEARCHES + " + :" + COLUMN_STAT_SEARCHES + ", "
          + COLUMN_STAT_DOCS     + " = " + COLUMN_STAT_DOCS     + " + :" + COLUMN_STAT_DOCS + ", "
          + COLUMN_STAT_WRITTEN  + " = " + COLUMN_STAT_WRITTEN  + " + :" + COLUMN_STAT_WRITTEN + ", "
          + COLUMN_STAT_ELAPSED  + " = " + COLUMN_STAT_ELAPSED  + " + :" + COLUMN_STAT_ELAPSED;

  private static ObjectMapper jsonMapper;

  public RestrictionsDAO() {
    Jackson2ObjectMapperFactoryBean factoryBean = new Jackson2ObjectMapperFactoryBean();
    factoryBean.setSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    factoryBean.setFeaturesToDisable(SerializationFeature.WRITE_NULL_MAP_VALUES);
    factoryBean.afterPropertiesSet();
    jsonMapper = factoryBean.getObject();
  }
  public ObjectMapper getJsonMapper() {
    return jsonMapper;
  }

  /*
   * RULE SETs
   *
   * SQL, methods and mappers related to rules set retrieval
   */
  @SqlQuery("SELECT id FROM " + TABLE_RULESET + " WHERE retired IS NULL AND activated IS NOT NULL")
  abstract Long getCurrentRulesetId();

  @SqlQuery("SELECT id FROM " + TABLE_RULESET + " WHERE activated IS NULL")
  abstract Long getNewRulesetId();

  @SqlQuery("SELECT * FROM " + TABLE_RULES + " WHERE rulesSetId = :rulesSetId ORDER BY id ASC")
  abstract List<CdxRule> getRules(@Bind("rulesSetId") long rulesSetId);

  public CdxAccessControl getCurrentRules() {
    Long ruleSetId = getCurrentRulesetId();
    if (ruleSetId == null) {
      return null;
    }
    List<CdxRule> rules = getRules(ruleSetId);
    return new CdxAccessControl(rules);
  }

  public CdxAccessControl getNewRules() {
    Long ruleSetId = getNewRulesetId();
    if (ruleSetId == null) {
      return null;
    }
    List<CdxRule> rules = getRules(ruleSetId);
    return new CdxAccessControl(rules);
  }

  // This MUST be public for JDBI... ignore your IDE hints
  public static class CdxRuleMapper implements ResultSetMapper<CdxRule> {
    @Override
    public CdxRule map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
      String json = rs.getString("ruleJson");
      try {
        return jsonMapper.readValue(json, CdxRule.class);
      } catch (IOException e) {
        throw new SQLException("Error parsing result from database", e);
      }
    }
  }

  /*
   * RUN MANAGEMENT
   *
   * SQL, methods and mappers related to querying and manipulating the 'last_run' table
   */
  @SqlQuery("SELECT * FROM " + TABLE_RUN + " ORDER BY started DESC LIMIT 1")
  public abstract LastRun getLastRun();

  // This MUST be public for JDBI... ignore your IDE hints
  public static class LastRunMapper implements ResultSetMapper<LastRun> {
    @Override
    public LastRun map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
      LastRun lastRun = new LastRun();
      lastRun.setId(rs.getLong("id"));
      lastRun.setStarted(asDate(rs, "started"));
      lastRun.setDateCompleted(asDate(rs, "dateCompleted"));
      lastRun.setAllCompleted(asDate(rs, "allCompleted"));
      lastRun.setProgressRuleId(rs.getLong("progressRuleId"));
      lastRun.setWorkRules(rs.getLong("workRules"));
      lastRun.setWorkSearches(rs.getLong("workSearches"));
      lastRun.setWorkDocuments(rs.getLong("workDocuments"));
      lastRun.setWorkWritten(rs.getLong("workWritten"));
      lastRun.setWorkMsElapsed(rs.getLong("workMsElapsed"));
    	return lastRun;
    }

    private Date asDate(ResultSet rs, String fieldName) throws SQLException {
      Timestamp ts = rs.getTimestamp(fieldName);
      if (ts == null) return null;
      return new Date(ts.getTime());
    }
  }

  @SqlQuery("SELECT * FROM " + TABLE_RUN + " WHERE id = :runId")
  public abstract LastRun getRunById(@Bind("runId") long runId);

  @SqlUpdate("INSERT INTO " + TABLE_RUN + " (started, progressRuleId, " + COLUMN_STATS_ALL
          + ") VALUES (NOW(), 0, 0, 0, 0, 0, 0)")
  @GetGeneratedKeys
  abstract Long newRunId();

  public LastRun startNewRun() {
    Long newRunId = newRunId();
    if (newRunId == null) {
      // JDBI should take care of this with @GetGeneratedKeys... but it failed once
      throw new IllegalStateException("Database error. Unable to read the ID of the row just written.");
    }
    return getRunById(newRunId);
  }

  @SqlQuery("SELECT progressRuleId FROM " + TABLE_RUN + " WHERE id = :runId")
  public abstract long getLastRunProgress(@Bind("runId") long runId);

  @SqlUpdate("UPDATE " + TABLE_RUN + " SET progressRuleId = :ruleId, "
          + COLUMN_STATS_UPDATE_SQL + " WHERE id = :runId")
  public abstract void updateRunProgress(@Bind("runId") long runId, @Bind("ruleId") long ruleId,
                                         @Bind("workSearches") long workSearches,
                                         @Bind("workDocuments") long workDocuments,
                                         @Bind("workWritten") long workWritten,
                                         @Bind("workMsElapsed") long workMsElapsed);

  @SqlUpdate("UPDATE " + TABLE_RUN + " SET progressRuleId = 0, dateCompleted = NOW()")
  public abstract void finishDateBasedRun(@Bind("runId") long runId);

  @SqlUpdate("UPDATE " + TABLE_RUN + " SET allCompleted = NOW()")
  public abstract void finishNightyRun(@Bind("runId") long runId);

  @SqlUpdate("INSERT INTO " + TABLE_RULESET + " (received) VALUES (NOW())")
  @GetGeneratedKeys
  abstract long newRuleset();

  @SqlUpdate("INSERT INTO " + TABLE_RULES + " (rulesSetId, id, ruleJson) VALUES (:rulesSetId, :id, :ruleJson)")
  @GetGeneratedKeys
  abstract long writeRule(@Bind("rulesSetId") long rulesSetId, @Bind("id") long id, @Bind("ruleJson") String ruleJson);

  @Transaction
  public void addNewRuleSet(CdxAccessControl ruleset) throws JsonProcessingException {
    long newRulesetId = newRuleset();
    for (CdxRule rule : ruleset.getRules().values()) {
      String json = jsonMapper.writeValueAsString(rule);
      writeRule(newRulesetId, rule.getId(), json);
    }
  }

  @SqlUpdate("UPDATE " + TABLE_RULESET + " SET retired = :activationTime WHERE retired IS NULL AND activated IS NOT NULL")
  abstract void retireCurrentRules(@Bind("activationTime") Date activationTime);

  @SqlUpdate("UPDATE " + TABLE_RULESET + " SET activated = :activationTime WHERE activated IS NULL")
  abstract void activateNewRules(@Bind("activationTime") Date activationTime);

  @Transaction
  public CdxAccessControl makeNewRuleSetCurrent(Date activationTime) {
    retireCurrentRules(activationTime);
    activateNewRules(activationTime);
    return getCurrentRules();
  }
}
