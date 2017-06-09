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

import java.text.SimpleDateFormat;
import java.util.Date;

public class LastRun {
  // Tracking data
  private long id;
  private Date started;
  private Date dateCompleted;
  private Date allCompleted;
  private long progressRuleId;
  // Stats
  private long workRules;
  private long workSearches;
  private long workDocuments;
  private long workWritten;
  private long workMsElapsed;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Date getStarted() {
    return started;
  }

  public void setStarted(Date started) {
    this.started = started;
  }

  public Date getDateCompleted() {
    return dateCompleted;
  }

  public void setDateCompleted(Date dateCompleted) {
    this.dateCompleted = dateCompleted;
  }

  public Date getAllCompleted() {
    return allCompleted;
  }

  public void setAllCompleted(Date allCompleted) {
    this.allCompleted = allCompleted;
  }

  public long getProgressRuleId() {
    return progressRuleId;
  }

  public void setProgressRuleId(long progressRuleId) {
    this.progressRuleId = progressRuleId;
  }

  public long getWorkRules() {
    return workRules;
  }

  public void setWorkRules(long workRules) {
    this.workRules = workRules;
  }

  public long getWorkSearches() {
    return workSearches;
  }

  public void setWorkSearches(long workSearches) {
    this.workSearches = workSearches;
  }

  public long getWorkDocuments() {
    return workDocuments;
  }

  public void setWorkDocuments(long workDocuments) {
    this.workDocuments = workDocuments;
  }

  public long getWorkWritten() {
    return workWritten;
  }

  public void setWorkWritten(long workWritten) {
    this.workWritten = workWritten;
  }

  public long getWorkMsElapsed() {
    return workMsElapsed;
  }

  public void setWorkMsElapsed(long workMsElapsed) {
    this.workMsElapsed = workMsElapsed;
  }

  // HTML here might look funny, but it goes into the dashboard... A more graceful way of handling this would be nice
  @Override
  public String toString() {
    SimpleDateFormat sdf = new SimpleDateFormat("dd MMM HH:mm Z");
    return "<br/>Start:" + date(sdf, started) + ",<br/>Dates:" + date(sdf, dateCompleted) + ",<br/>Finish:" + date(sdf, allCompleted)
            + ",<br/>Rules:(" + workRules + "),<br/>Elapsed:(" + workMsElapsed + ")";
  }

  private String date(SimpleDateFormat sdf, Date date) {
    if (date == null) {
      return "(NULL)";
    }
    return sdf.format(date);
  }
}
