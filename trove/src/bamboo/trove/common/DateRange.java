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
	

	@Override
	public int hashCode(){
		final int prime = 31;
		int result = 1;
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj){
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		DateRange other = (DateRange) obj;
		if (end == null){
			if (other.end != null) return false;
		}
		else if (!end.equals(other.end)) return false;
		if (start == null){
			if (other.start != null) return false;
		}
		else if (!start.equals(other.start)) return false;
		return true;
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
	@Override
	public String toString(){
		// TODO Auto-generated method stub
		return this.getStart() + " TO " + this.getEnd();
	}
}
