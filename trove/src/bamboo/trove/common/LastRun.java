package bamboo.trove.common;

import java.util.Date;

public class LastRun{
	private Date date;
	private Boolean finished;
	
	public LastRun(Date date, Boolean finished){
		this.date = date;
		this.finished = finished;
	}
	public Date getDate(){
		return date;
	}
	public void setDate(Date date){
		this.date = date;
	}
	public Boolean isFinished(){
		return finished;
	}
	public void setFinished(Boolean finished){
		this.finished = finished;
	}
	
	@Override
	public String toString(){
		return date + (finished?"":" not")+ " finished.";
	}
}
