package bamboo.trove.rule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Record{
	List<String> otherIds;
	String id;
	String url;
	String site;
	List<Date> date;
	String contentType;
	String title;
	List<String> text;
	boolean textError;
	
	public Record(String id, String url, String site, List<Date> date, 
			String contentType, String title, List<String> text, boolean textError){
		this.id = id;
		this.url = url;
		this.site = site;
		this.date = date;
		this.contentType = contentType;
		this.title = title;
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
	public List<String> getText(){
		return text;
	}
	public boolean getTextError(){
		return textError;
	}

	public List<String> getOtherIds(){
		return otherIds;
	}
	public void addOtherId(String otherId){
		if(otherIds == null){
			otherIds = new ArrayList<>();
		}
		otherIds.add(otherId);
	}
	/**
	 * Is this the complete record.
	 * <p/>
	 * Or is it only part(when searching for rules to change only minimum data
	 * read).
	 * 
	 * @return
	 */
	public boolean isFullRecord(){
		return site != null;
	}
}
