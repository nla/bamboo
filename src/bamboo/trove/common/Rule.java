package bamboo.trove.common;

import java.util.Date;

import org.archive.url.SURT;

public class Rule{
	private int id;
	private DocumentStatus policy;
	private Date lastUpdated;
	private long embargoTime;
	private DateRange captureRange;
	private DateRange viewRange;
	private String surt;
	
	public Rule(int id, DocumentStatus policy, Date lastUpdated, long embargo, 
			Date captureStart, Date captureEnd, Date viewStart, Date viewEnd, String surt){
		if(id <= 0){
			throw new IllegalArgumentException("Invalid id.");
		}
		if(policy == null){
			throw new NullPointerException("Must have a policy.");			
		}
		if(lastUpdated == null){
			throw new NullPointerException("Must have a lastUpdated date.");
		}
		if(embargo < 0){
			throw new IllegalArgumentException("Embargo can not be negitave.");			
		}
		if(surt == null){
			throw new NullPointerException("Must have a SURT string.");			
		}
		if((captureStart != null || captureEnd != null)
				&& (viewStart != null || viewEnd != null)){
			throw new IllegalArgumentException("Can not have a range both view and capture.");			
		}
		
		this.id = id;
		this.policy = policy;
		this.lastUpdated = lastUpdated;
		this.embargoTime = embargo;
		this.surt = surt;
		if(captureStart != null || captureEnd != null){
			this.captureRange = new DateRange(captureStart, captureEnd);
		}
		if(viewStart != null || viewEnd != null){
			this.viewRange = new DateRange(viewStart, viewEnd);
		}
	}
	
	/** 
	 * Check if this URL and capture date meets this rule.
	 * 
	 * @param url
	 * @param captureDate
	 * @return
	 */
	public boolean matches(String url, Date captureDate){
		String s = SURT.toSURT(url);
		if(embargoTime > 0){
			Date d = new Date(System.currentTimeMillis() - embargoTime);
			if(captureDate.after(d)){
				return false;
			}
		}
		if(captureRange != null){
			if(! captureRange.isDateInRange(captureDate)){
				return false;
			}
		}
		if(viewRange != null){
			if(! viewRange.isDateInRange(new Date())){
				return false;
			}
		}
		if(!s.startsWith(surt)){
			return false;
		}
		return true;
	}

	public int getId(){
		return id;
	}
	public Date getLastUpdated(){
		return lastUpdated;
	}
	public DocumentStatus getPolicy(){
		return policy;
	}
}
