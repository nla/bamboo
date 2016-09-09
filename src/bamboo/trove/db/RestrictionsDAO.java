package bamboo.trove.db;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.skife.jdbi.v2.SQLStatement;
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

import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.Rule;

@RegisterMapper({RestrictionsDAO.CollectionRuleMapper.class})
public abstract class RestrictionsDAO implements Transactional<RestrictionsDAO> {
  public static final String TABLE = "restriction_rule_web_archives";
  
  @SqlQuery("select * from "+TABLE+ " where status = :status")
  protected abstract List<Rule> getRules(@Bind("status") String status);
  public List<Rule> getCurrentRules(){
  	return getRules("c");
  }
  public List<Rule> getNewRules(){
  	return getRules("n");
  }
  
  @SqlBatch("insert into " + TABLE
  		+ "(id, status, last_updated, surt, policy, embargo, captured_start, captured_end, retrieved_start, retrieved_end) "
  		+ "VALUES (:id, 'n', :lastUpdated, :surt, :policy, :embargo, :capturedRangeStart, :capturedRangeEnd, "
  		+ ":retrievedRangeStart, :retrievedRangeEnd)")
  public abstract void addNewRuleSet(@RuleBinder List<Rule> rule);
  
  @SqlUpdate("delete from " + TABLE + " where status = 'p'")
  protected abstract void removePreviousRuleSet();
  
  @SqlUpdate("update " + TABLE + " set status = 'p' where status = 'c'")
  protected abstract void makeCurrentRuleSetPrevious();
  
  @SqlUpdate("update " + TABLE + " set status = 'c' where status = 'n'")
  protected abstract void makeNewRuleSetCurrent();

  @Transaction
  public void makeNewRulesCurrent(){
  	removePreviousRuleSet();
  	makeCurrentRuleSetPrevious();
  	makeNewRuleSetCurrent();
  }
  

  public static class CollectionRuleMapper implements ResultSetMapper<Rule> {
    @Override
    public Rule map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    	System.out.println("match_exact : "+"t".equals(rs.getString("match_exact")));
    	return new Rule(rs.getInt("id"), DocumentStatus.valueOf(rs.getString("policy")), 
    				rs.getTimestamp("last_updated"), rs.getLong("embargo"),
    				rs.getTimestamp("captured_start"), rs.getTimestamp("captured_end"), 
    				rs.getTimestamp("retrieved_start"), rs.getTimestamp("retrieved_end"),
    				rs.getString("surt"), "t".equals(rs.getString("match_exact")));
    }
  }

  @BindingAnnotation(RestrictionsDAO.RuleBinder.RuleBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  public @interface RuleBinder{
  	public static class RuleBinderFactory implements BinderFactory{
  		public Binder<RuleBinder, Rule> build(Annotation annotation){
  			return new Binder<RuleBinder, Rule>(){
					@Override
  				public void bind(SQLStatement<?> q, RuleBinder bind, Rule r){
  					q.bind("id", r.getId());
  					q.bind("lastUpdated", r.getLastUpdated());
  					q.bind("surt", r.getSurt());
  					q.bind("policy", r.getPolicy().toString());
  					q.bind("embargo", r.getEmbargoTime());
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
  				}
  			};
  		}
  	}
  }
}
