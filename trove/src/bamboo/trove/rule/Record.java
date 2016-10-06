package bamboo.trove.rule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Record{
	String id;
	String url;
	String site;
	List<Date> date;
	String contentType;
	String title;
	List<Integer> ruleIds;
	List<String> text;
	boolean textError;
	int deleteCount = 0;
	
	public Record(String id, String url, String site, List<Date> date, 
			String contentType, String title, List<Integer> ruleIds, List<String> text, boolean textError){
		this.id = id;
		this.url = url;
		this.site = site;
		this.date = date;
		this.contentType = contentType;
		this.title = title;
		this.ruleIds = ruleIds;
		this.text = text;
		this.textError = textError;
	}

	public String getId(){
		return id;
	}
	public String getUrl(){
		return url;
	}
	public String getSite(){
		return site;
	}
	public List<Date> getDate(){
		return date;
	}
	public String getContentType(){
		return contentType;
	}
	public String getTitle(){
		return title;
	}
	public List<Integer> getRuleIds(){
		return ruleIds;
	}
	public List<String> getText(){
		return text;
	}
	public boolean getTextError(){
		return textError;
	}
	public int getDeleteCount(){
		return deleteCount;
	}
	public void setDeleteCount(int deleteCount){
		this.deleteCount = deleteCount;
	}
	public boolean isDeleteNeeded(){
		return (deleteCount > 0);
	}
	/**
	 * Was the record split.
	 * <p/>
	 * The record was matched to more than one rule so was split(for each rule it matched).
	 * 
	 * @return
	 */
	public boolean wasSplit(){
		return ruleIds.size() != 1;
	}
}
