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
	private String url = "";
	
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
		convertSurt();
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

	private void convertSurt(){
		// remove protocol
		surt = surt.replaceFirst("^.*://", "");
		// remove port
		surt = surt.replaceFirst(":\\d{1,4}", "");
		if(!surt.startsWith("(")){
			throw new IllegalAccessError("SURT must start with (");
		}
		// remove ( from the front
		String txt = surt.substring(1);
		int pos = txt.indexOf(")");
		
		// separate the host from the path
		String path = "";
		if(pos >= 0){
			path = txt.substring(pos+1);
			txt = txt.substring(0, pos);
			if(!path.startsWith("/")){
				path = "/" + path;
			}
		}
		
		// remove the , from the end of the host
		if(txt.endsWith(",")){
			txt = txt.substring(0, txt.length()-1);
		}
		
		// put the host in the correct order for a url
		for(String u : txt.split(",")){
			if(!url.isEmpty()){
				url = "." + url;
			}
			url = u + url;
		}
		url += path;
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
	public String getSurt(){
		return surt;
	}
	public String getUrl(){
		return url;
	}
	public DateRange getViewRange(){
		return viewRange;
	}
	public DateRange getCaptureRange(){
		return captureRange;
	}
	public long getEmbargoTime(){
		return embargoTime;
	}
}
