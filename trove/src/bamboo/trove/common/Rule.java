package bamboo.trove.common;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

import org.archive.url.SURT;

public class Rule implements Comparable<Rule>{
	private int id;
	private DocumentStatus policy;
	private Date lastUpdated;
	private long embargo;
	private DateRange capturedRange;
	private DateRange retrievedRange;
	private String surt;
	private boolean matchExact = false;
	private String url = "";
	
	public Rule(int id, DocumentStatus policy, Date lastUpdated, long embargo, 
			Date captureStart, Date captureEnd, Date viewStart, Date viewEnd, String surt, boolean matchExact){
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
		
		this.id = id;
		this.policy = policy;
		this.lastUpdated = lastUpdated;
		this.embargo = embargo;
		this.surt = surt;
		this.matchExact = matchExact;
		if(captureStart != null || captureEnd != null){
			this.capturedRange = new DateRange(captureStart, captureEnd);
		}
		if(viewStart != null || viewEnd != null){
			this.retrievedRange = new DateRange(viewStart, viewEnd);
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
		String s = "(";
		try{
			URL u = new URL(url);
			s = u.getHost() + u.getPath();
			s = SURT.toSURT(s);
		}
		catch (MalformedURLException e){
			// should match to default catch all rule
		}

		if(embargo > 0){
			Date d = new Date(System.currentTimeMillis() - (embargo*1000));
			if(!captureDate.after(d)){
				return false;
			}
		}
		if(capturedRange != null){
			if(! capturedRange.isDateInRange(captureDate)){
				return false;
			}
		}
		if(retrievedRange != null){
			if(! retrievedRange.isDateInRange(new Date())){
				return false;
			}
		}
		if(matchExact){
			if(!s.equals(surt)){
				return false;
			}
		}
		else{
  		if(!s.startsWith(surt)){
  			return false;
  		}
		}
		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private int comp(Comparable mine, Comparable other){
   	if ((mine == null) && (other == null)) {
  		return 0;
  	}  	
  	if ((mine != null) && (other != null)) {
  		return -mine.compareTo(other);
  	}
  	return -((mine == null) ? -1 : 1);
	}
	
	@Override
	public int compareTo(Rule o){
		int ret = -comp(this.getSurt(), o.getSurt());
		if(ret == 0){
			ret = comp(this.isMatchExact(), o.isMatchExact());
		}
		if(ret == 0){
			if(this.getCapturedRange()!=null){
				ret = comp(this.getCapturedRange().getStart(), o.getCapturedRange() != null ? o.getCapturedRange().getStart():null);
				if(ret == 0){
					ret = comp(this.getCapturedRange().getEnd(), o.getCapturedRange() != null ? o.getCapturedRange().getEnd():null);
				}
			}
		}
		if(ret == 0){
			if(this.getRetrievedRange()!=null){
				ret = comp(this.getRetrievedRange().getStart(), o.getRetrievedRange() != null ? o.getRetrievedRange().getStart():null);
				if(ret == 0){
					ret = comp(this.getRetrievedRange().getEnd(), o.getRetrievedRange() != null ? o.getRetrievedRange().getEnd():null);
				}
			}
		}
		if(ret == 0){
			ret = -comp(this.getEmbargo(), o.getEmbargo());
		}
		return ret;
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
	
	
	@Override
	public int hashCode(){
		final int prime = 31;
		int result = 1;
		result = prime * result + ((capturedRange == null) ? 0 : capturedRange.hashCode());
		result = prime * result + (int) (embargo ^ (embargo >>> 32));
		result = prime * result + id;
		result = prime * result + ((lastUpdated == null) ? 0 : lastUpdated.hashCode());
		result = prime * result + (matchExact ? 1231 : 1237);
		result = prime * result + ((policy == null) ? 0 : policy.hashCode());
		result = prime * result + ((retrievedRange == null) ? 0 : retrievedRange.hashCode());
		result = prime * result + ((surt == null) ? 0 : surt.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj){
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Rule other = (Rule) obj;
		if (capturedRange == null){
			if (other.capturedRange != null) return false;
		}
		else if (!capturedRange.equals(other.capturedRange)) return false;
		if (embargo != other.embargo) return false;
		if (id != other.id) return false;
		if (lastUpdated == null){
			if (other.lastUpdated != null) return false;
		}
		else if (!lastUpdated.equals(other.lastUpdated)) return false;
		if (matchExact != other.matchExact) return false;
		if (policy != other.policy) return false;
		if (retrievedRange == null){
			if (other.retrievedRange != null) return false;
		}
		else if (!retrievedRange.equals(other.retrievedRange)) return false;
		if (surt == null){
			if (other.surt != null) return false;
		}
		else if (!surt.equals(other.surt)) return false;
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
	public String getSurt(){
		return surt;
	}
	public String getUrl(){
		return url;
	}
	public DateRange getRetrievedRange(){
		return retrievedRange;
	}
	public DateRange getCapturedRange(){
		return capturedRange;
	}
	public long getEmbargo(){
		return embargo;
	}
	public boolean isMatchExact(){
		return matchExact;
	}
}