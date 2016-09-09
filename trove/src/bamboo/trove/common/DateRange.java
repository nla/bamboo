package bamboo.trove.common;

import java.util.Date;

/**
 * Holds a two dates to be a range.
 * 
 * @author icaldwell
 *
 */
public class DateRange{
	private Date start;
	private Date end;
	
	/**
	 * Create a range can be open ended(start or end can be null). 
	 * @param start
	 * @param end
	 */
	public DateRange(Date start, Date end){
		if(start == null && end == null){
			throw new NullPointerException("Both start and end cannot be null.");
		}
		if(!isValidRange(start, end)){
			throw new IllegalArgumentException("End date must be after Start date.");
		}
		this.start = start;
		this.end = end;
	}
	
	/**
	 * Check if the date fits in this range.
	 * 
	 * @param date
	 * @return
	 */
	public boolean isDateInRange(Date date){
		if(date == null){
			throw new NullPointerException("Date cannot be null.");
		}
		if(start == null){
			return ! date.after(end);
		}
		if(end == null){
			return ! date.before(start);
		}
		boolean before = ! date.after(end);
		boolean after = ! date.before(start);
		return before && after;
	}
	public Date getStart(){
		return start;
	}
	
	public Date getEnd(){
		return end;
	}
	
	/**
	 * Check that these dates are valid for use in a dateRange.
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public static boolean isValidRange(Date start, Date end){
		if(start == null && end == null){
			return false;
		}
		if(start == null){
			return true;
		}
		if(end == null){
			return true;
		}
		if(end.before(start)){
			return false;
		}
		return true;
	}
}
