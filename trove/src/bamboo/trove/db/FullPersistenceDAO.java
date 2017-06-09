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
  String ID_TABLE = "index_persistance_web_archives";
  String ID_COLUMN = "last_warc_id";
  String MOD_COLUMN = "modulo_remainder";
  String PERIODIC_TABLE = "index_persistance_web_archives_pending";
  String PREIODIC_ID_COLUMN = "id";
  String RESUME_COLUMN = "resumption_token";
  String ERROR_TABLE = "index_persistance_web_archives_errors";
  String ERROR_ID_COLUMN = "warc_id";
  String ERROR_RETRY_COLUMN = "retries";
  String ERROR_TIME_COLUMN = "last_error";
  String ERROR_DOMAIN_COLUMN = "domain";

  class ErrorMapper implements ResultSetMapper<Pair<Timestamp, Integer>> {
    @Override
    public Pair<Timestamp, Integer> map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
      return new Pair<>(resultSet.getTimestamp(1), resultSet.getInt(2));
    }
  }

  class OldErrorMapper implements ResultSetMapper<OldError> {
    @Override
    public OldError map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
      return new OldError(resultSet.getLong(1), new Pair<>(resultSet.getTimestamp(2), resultSet.getInt(3)),
              resultSet.getString(4));
    }
  }

  @SqlUpdate("INSERT INTO " + ID_TABLE + " (" + ID_COLUMN + ", " + MOD_COLUMN + ") VALUES (:lastId, :moduloRemainder)"
          + " ON DUPLICATE KEY UPDATE " + ID_COLUMN + " = :lastId")
  void updateLastId(@Bind("lastId") long lastId, @Bind("moduloRemainder") int moduloRemainder);

  @SqlQuery("SELECT " + ID_COLUMN + " FROM " + ID_TABLE + " WHERE " + MOD_COLUMN + " = :moduloRemainder")
  long getLastId(@Bind("moduloRemainder") int moduloRemainder);

  @SqlUpdate("INSERT INTO " + PERIODIC_TABLE + " (" + PREIODIC_ID_COLUMN + ", " + RESUME_COLUMN + ") VALUES (1, :resumptionToken)"
      + " ON DUPLICATE KEY UPDATE " + RESUME_COLUMN + " = :resumptionToken")
  void updateResumptionToken(@Bind("resumptionToken") String resumptionToken);

  @SqlQuery("SELECT " + RESUME_COLUMN + " FROM " + PERIODIC_TABLE + " WHERE " + PREIODIC_ID_COLUMN + " = 1")
  String getResumptionToken();

  @SqlUpdate("INSERT INTO " + ERROR_TABLE + " (" + ERROR_ID_COLUMN + ", " + ERROR_DOMAIN_COLUMN 
      + ") VALUES (:warcId, :domain)" 
      + " ON DUPLICATE KEY UPDATE " + ERROR_RETRY_COLUMN + " = " + ERROR_RETRY_COLUMN + " + 1")
  void trackError(@Bind("warcId") long warcId, @Bind("domain") String domain);

  @SqlUpdate("DELETE FROM " + ERROR_TABLE + " WHERE " + ERROR_ID_COLUMN + " = :warcId")
  void removeError(@Bind("warcId") long warcId);

  @SqlQuery("SELECT " + ERROR_ID_COLUMN + ", " + ERROR_TIME_COLUMN + ", " + ERROR_RETRY_COLUMN + ", "
          + ERROR_DOMAIN_COLUMN + " FROM " + ERROR_TABLE + " WHERE " +  ERROR_ID_COLUMN + " = :warcId")
  OldError checkError(@Bind("warcId") long warcId);

  @SqlQuery("SELECT " + ERROR_ID_COLUMN + ", " + ERROR_TIME_COLUMN + ", " + ERROR_RETRY_COLUMN + ", " + ERROR_DOMAIN_COLUMN
          + " FROM " + ERROR_TABLE + " ORDER BY " +  ERROR_ID_COLUMN + " desc")
  List<OldError> oldErrors();

  class OldError {
    public final Long warcId;
    public final Pair<Timestamp, Integer> error;
    final Domain domain;
    OldError(Long warcId, Pair<Timestamp, Integer> error, String domain) {
      this.warcId = warcId;
      this.error = error;
      this.domain = Domain.decode(domain);
    }
  }
  
  enum Domain{
    FULL("f", "Full"),
    PERIODIC("p", "Periodic");

    private String display;
    private String code;

    Domain(String code, String display){
      this.code = code;
      this.display = display;
    }

    public String getCode(){
      return code;
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
