/*
 * Copyright 2016-2017 National Library of Australia
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
