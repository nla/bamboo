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
package bamboo.trove.db;

import org.apache.commons.math3.util.Pair;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@RegisterMapper({FullPersistenceDAO.ErrorMapper.class, FullPersistenceDAO.OldErrorMapper.class})
public interface FullPersistenceDAO {
  public static final String ID_TABLE = "index_persistance_web_archives";
  public static final String ID_COLUMN = "last_warc_id";
  public static final String MOD_COLUMN = "modulo_remainder";
  public static final String PERIODIC_TABLE = "index_persistance_web_archives_pending";
  public static final String PREIODIC_ID_COLUMN = "id";
  public static final String RESUME_COLUMN = "resumption_token";
  public static final String ERROR_TABLE = "index_persistance_web_archives_errors";
  public static final String ERROR_ID_COLUMN = "warc_id";
  public static final String ERROR_RETRY_COLUMN = "retries";
  public static final String ERROR_TIME_COLUMN = "last_error";
  public static final String ERROR_DOMAIN_COLUMN = "domain";

  class ErrorMapper implements ResultSetMapper<Pair<Timestamp, Integer>> {
    @Override
    public Pair<Timestamp, Integer> map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
      return new Pair<>(resultSet.getTimestamp(1), resultSet.getInt(2));
    }
  }

  class OldErrorMapper implements ResultSetMapper<OldError> {
    @Override
    public OldError map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
      return new OldError(resultSet.getLong(1), new Pair<>(resultSet.getTimestamp(2), resultSet.getInt(3))
      		, resultSet.getString(4));
    }
  }

  @SqlUpdate("INSERT INTO " + ID_TABLE + " (" + ID_COLUMN + ", " + MOD_COLUMN + ") VALUES (:lastId, :moduloRemainder)"
          + " ON DUPLICATE KEY UPDATE " + ID_COLUMN + " = :lastId")
  public void updateLastId(@Bind("lastId") long lastId, @Bind("moduloRemainder") int moduloRemainder);

  @SqlQuery("SELECT " + ID_COLUMN + " FROM " + ID_TABLE + " WHERE " + MOD_COLUMN + " = :moduloRemainder")
  public long getLastId(@Bind("moduloRemainder") int moduloRemainder);

  @SqlUpdate("INSERT INTO " + PERIODIC_TABLE + " (" + PREIODIC_ID_COLUMN + ", " + RESUME_COLUMN + ") VALUES (1, :resumptionToken)"
      + " ON DUPLICATE KEY UPDATE " + RESUME_COLUMN + " = :resumptionToken")
  public void updateResumptionToken(@Bind("resumptionToken") String resumptionToken);

  @SqlQuery("SELECT " + RESUME_COLUMN + " FROM " + PERIODIC_TABLE + " WHERE " + PREIODIC_ID_COLUMN + " = 1")
  public String getResumptionToken();

  @SqlUpdate("INSERT INTO " + ERROR_TABLE + " (" + ERROR_ID_COLUMN + ", " + ERROR_DOMAIN_COLUMN 
      + ") VALUES (:warcId, :domain)" 
      + " ON DUPLICATE KEY UPDATE " + ERROR_RETRY_COLUMN + " = " + ERROR_RETRY_COLUMN + " + 1")
  public void trackError(@Bind("warcId") long warcId, @Bind("domain") String domain);

  @SqlUpdate("DELETE FROM " + ERROR_TABLE + " WHERE " + ERROR_ID_COLUMN + " = :warcId")
  public void removeError(@Bind("warcId") long warcId);

  @SqlQuery("SELECT " + ERROR_TIME_COLUMN + ", " + ERROR_RETRY_COLUMN + ", " + ERROR_DOMAIN_COLUMN+ ", " + ERROR_DOMAIN_COLUMN
      + " FROM " + ERROR_TABLE
      + " WHERE " +  ERROR_ID_COLUMN + " = :warcId")
  public OldError checkError(@Bind("warcId") long warcId);

  @SqlQuery("SELECT " + ERROR_ID_COLUMN + ", " + ERROR_TIME_COLUMN + ", " + ERROR_RETRY_COLUMN + ", " + ERROR_DOMAIN_COLUMN
          + " FROM " + ERROR_TABLE + " ORDER BY " +  ERROR_ID_COLUMN + " desc")
  public List<OldError> oldErrors();

  public class OldError {
    public final Long warcId;
    public final Pair<Timestamp, Integer> error;
    public final Domain domain;
    public OldError(Long warcId, Pair<Timestamp, Integer> error, String domain) {
      this.warcId = warcId;
      this.error = error;
      this.domain = Domain.decode(domain);
    }
  }
  
  public enum Domain{
    FULL("f", "Full"),
    PERIODIC("p", "Periodic");

    private String display;
    private String code;

    private Domain(String code, String display){
      this.code = code;
      this.display = display;
    }

    public String getCode(){
      return code;
    }
    public String getDisplay(){
      return display;
    }
    @Override
    public String toString(){
      return display;
    }
    
    public static Domain decode(String code){
      if("f".equals(code)){
        return FULL;
      }
      if("p".equals(code)){
        return PERIODIC;
      }
      return null;
    }
  }
}
