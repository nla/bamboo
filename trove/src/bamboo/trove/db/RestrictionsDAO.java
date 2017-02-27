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
package bamboo.trove.db;

import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.LastRun;
import bamboo.trove.common.Rule;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.skife.jdbi.v2.sqlobject.BindingAnnotation;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

@RegisterMapper({RestrictionsDAO.CollectionRuleMapper.class, RestrictionsDAO.CollectionLastRunMapper.class})
public abstract class RestrictionsDAO implements Transactional<RestrictionsDAO> {
  private static final String TABLE     = "restriction_rule_web_archives";
  private static final String TABLE_RUN = "restriction_rule_last_run_web_archives";
  
  @SqlQuery("select * from "+TABLE+ " where status = :status")
  abstract List<Rule> getRules(@Bind("status") String status);
  public List<Rule> getCurrentRules(){
  	return getRules("c");
  }
  public List<Rule> getNewRules(){
  	return getRules("n");
  }
  
  @SqlQuery("select last_run, finished from " + TABLE_RUN + " where id = 1")
  public abstract LastRun getLastRun();
  
  @SqlUpdate("update " + TABLE_RUN + " set last_run = :lastRun, finished = :finished where id = 1")
  public abstract void setLastRun(@LastRunBinder() LastRun lastRun);
  
  @SqlBatch("insert into " + TABLE
  		+ "(id, status, last_updated, surt, policy, embargo, captured_start, "
  		+ "captured_end, retrieved_start, retrieved_end, match_exact) "
  		+ "VALUES (:id, 'n', :lastUpdated, :surt, :policy, :embargo, :capturedRangeStart, :capturedRangeEnd, "
  		+ ":retrievedRangeStart, :retrievedRangeEnd, :matchExact)")
  public abstract void addNewRuleSet(@RuleBinder List<Rule> rule);
  
  @SqlUpdate("delete from " + TABLE + " where status = 'p'")
  abstract void removePreviousRuleSet();
  
  @SqlUpdate("update " + TABLE + " set status = 'p' where status = 'c'")
  abstract void makeCurrentRuleSetPrevious();
  
  @SqlUpdate("update " + TABLE + " set status = 'c' where status = 'n'")
  abstract void makeNewRuleSetCurrent();

  @Transaction
  public void makeNewRulesCurrent(LastRun lastRun){
  	removePreviousRuleSet();
  	makeCurrentRuleSetPrevious();
  	makeNewRuleSetCurrent();
  	setLastRun(lastRun);
  }
  

  static class CollectionRuleMapper implements ResultSetMapper<Rule> {
    @Override
    public Rule map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    	return new Rule(rs.getInt("id"), DocumentStatus.valueOf(rs.getString("policy")), 
    				date(rs.getTimestamp("last_updated")), rs.getLong("embargo"),
    				date(rs.getTimestamp("captured_start")), date(rs.getTimestamp("captured_end")), 
    						date(rs.getTimestamp("retrieved_start")), date(rs.getTimestamp("retrieved_end")),
    				rs.getString("surt"), "t".equals(rs.getString("match_exact")));
    }
    private Date date(Timestamp t){
    	if(t == null){
    		return null;
    	}
    	return new Date(t.getTime());
    }
  }

  static class CollectionLastRunMapper implements ResultSetMapper<LastRun> {
    @Override
    public LastRun map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    	return new LastRun(new Date(rs.getTimestamp("last_run").getTime()), "t".equals(rs.getString("finished")));
    }
  }

//  @BindingAnnotation(RestrictionsDAO.DateBinder.DateBinderFactory.class)
//  @Retention(RetentionPolicy.RUNTIME)
//  @Target({ElementType.PARAMETER})
//  public @interface DateBinder{
//    String value() default "it";  
//
//  	public static class DateBinderFactory implements BinderFactory{
//  		public Binder<DateBinder, Date> build(Annotation annotation){
//  			return new Binder<DateBinder, Date>(){
//					@Override
//  				public void bind(SQLStatement<?> q, DateBinder bind, Date d){
//						q.bind(bind.value(), d);
//					}
//  			};
//  		}
//  	}
//  }
  
  @BindingAnnotation(RestrictionsDAO.RuleBinder.RuleBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  @interface RuleBinder{
  	class RuleBinderFactory implements BinderFactory{
  		public Binder<RuleBinder, Rule> build(Annotation annotation){
  			return (q, bind, r) -> {
          q.bind("id", r.getId());
          q.bind("lastUpdated", r.getLastUpdated());
          q.bind("surt", r.getSurt());
          q.bind("policy", r.getPolicy().toString());
          q.bind("embargo", r.getEmbargo());
          q.bind("matchExact", r.isMatchExact()?"t":"f");
          if(r.getCapturedRange() == null){
            q.bind("capturedRangeStart", (Date)null);
            q.bind("capturedRangeEnd", (Date)null);
          }
          else{
            q.bind("capturedRangeStart", r.getCapturedRange().getStart());
            q.bind("capturedRangeEnd", r.getCapturedRange().getEnd());
          }
          if(r.getRetrievedRange() == null){
            q.bind("retrievedRangeStart", (Date)null);
            q.bind("retrievedRangeEnd", (Date)null);
          }
          else{
            q.bind("retrievedRangeStart", r.getRetrievedRange().getStart());
            q.bind("retrievedRangeEnd", r.getRetrievedRange().getEnd());
          }
        };
  		}
  	}
  }
  
  @BindingAnnotation(RestrictionsDAO.LastRunBinder.LastRunBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  @interface LastRunBinder{
  	class LastRunBinderFactory implements BinderFactory{
  		public Binder<LastRunBinder, LastRun> build(Annotation annotation){
  			return (q, bind, r) -> {
          q.bind("lastRun", r.getDate());
          q.bind("finished", r.isFinished()?"t":"f");
        };
  		}
  	}
  }
}
