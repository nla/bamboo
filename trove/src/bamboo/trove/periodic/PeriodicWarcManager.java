/*
 * Copyright 2017 National Library of Australia
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
package bamboo.trove.periodic;

import bamboo.task.WarcToIndexResumption;
import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.ToIndex;
import bamboo.trove.db.FullPersistenceDAO;
import bamboo.trove.full.FullReindexWarcManager;
import bamboo.trove.rule.RuleChangeUpdateManager;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

@Service
public class PeriodicWarcManager extends FullReindexWarcManager {

  // We don't really need this, but we want Spring to start it before us, so we list it as a dependency
  @SuppressWarnings("unused")
  @Autowired(required = true)
  private RuleChangeUpdateManager ruleChangeUpdateManager;

  private String resumptionToken;
  private String bambooCollectionsSyncUrl; 
  private boolean limitRunningTime = true;
  private boolean stopPressed = false;
  private int startHour = 0;
  private int startMinute = 0;
  private int stopHour = 0;
  private int stopMinute = 0;
  private String runMessage = "";
  private boolean pStarting = false;
  private boolean pStopping = false;
  private Object startStopLock = new Object();

  public PeriodicWarcManager(){
  	log = LoggerFactory.getLogger(PeriodicWarcManager.class);
	}
  
  public void setLimitRunningTime(boolean limitRunningTime){
    this.limitRunningTime = limitRunningTime;
  }
  public void setLimitStartHour(int startHour){
    if(startHour < 0 || startHour > 23){
      throw new IllegalArgumentException("Hour must be between 0 and 23");
    }
    this.startHour = startHour;
  }
  public void setLimitStartMinute(int startMinute){
    if(startMinute < 0 || startMinute > 59){
      throw new IllegalArgumentException("Minute must be between 0 and 59");
    }
    this.startMinute = startMinute;
  }
  public void setLimitStopHour(int stopHour){
    if(stopHour < 0 || stopHour > 23){
      throw new IllegalArgumentException("Hour must be between 0 and 23");
    }
    this.stopHour = stopHour;
  }
  public void setLimitStopMinute(int stopMinute){
    if(stopMinute < 0 || stopMinute > 59){
      throw new IllegalArgumentException("Minute must be between 0 and 59");
    }
    this.stopMinute = stopMinute;
  }

  @PostConstruct
  public void init() throws InterruptedException {
    log.info("***** PeriodicWarcManager Start *****");
    super.init(false);
    resumptionToken = dao.getResumptionToken();
    bambooCollectionsSyncUrl = getBambooApiBaseUrl() + "collections/" + bambooCollectionId + "/warcs/sync";
    log.info("***** PeriodicWarcManager *****");
    log.info("Run at start              : {}", runAtStart);
    log.info("Resumption token          : {}", resumptionToken);
    log.info("Bamboo batch size         : {}", bambooBatchSize);
    log.info("Limit run between times   : {}", limitRunningTime);
    log.info("Run start time            : {}:{}", startHour, minuteTxt(startMinute));
    log.info("Run finish time           : {}:{}", stopHour, minuteTxt(stopMinute));
  }
  
  @Override
  public String getName(){
    return "Web Archives Periodic Indexing";
  }

  @Override
  public String getLastIdProcessed(){
    return resumptionToken + runMessage;
  }

  @Override
  protected LinkedList<ToIndex> getNextBatch() throws IOException{
    URL url = new URL(bambooCollectionsSyncUrl + "?after=" + resumptionToken + "&limit=" + bambooBatchSize);
    log.info("Contacting Bamboo for more IDs. after={}, limit={}", resumptionToken, bambooBatchSize);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty("Accept-Encoding", "gzip");

    InputStream in = new BufferedInputStream(connection.getInputStream());
    if("gzip".equals(connection.getHeaderField("Content-Encoding"))){
      in = new GZIPInputStream(in);
    }

    ObjectMapper om = getObjectMapper();
    JsonParser json = createParser(in);
    JsonToken token = json.nextToken();

    if (token == null) {
      throw new IllegalArgumentException("No JSON data found in response");
    }
    if (!JsonToken.START_ARRAY.equals(token)) {
      throw new IllegalArgumentException("JSON response is not an array");
    }

    LinkedList<ToIndex> result = new LinkedList<>();
    while (json.nextToken() == JsonToken.START_OBJECT) {
      WarcToIndexResumption wti = om.readValue(json, WarcToIndexResumption.class);
      ToIndex nextWarc = new ToIndex(wti);
      result.add(nextWarc);
      resumptionToken = wti.getResumptionToken();
    }

    log.info("Received {} objects from Bamboo.", result.size());
    return result;
  }
  
  @Override
  protected void updateLastId(ToIndex warc){
    dao.updateResumptionToken(warc.getResumptionToken());
  }
  @Override
  protected void updateTrackError(long id){
    dao.trackError(id, FullPersistenceDAO.Domain.PERIODIC.getCode());
  }
  
  /**
   * Check that the periodic indexer can run now.
   * <p>
   * That we are within the run time window.
   * 
   * @return
   */
  protected boolean canRunTime(){
    return canRunTime(Calendar.getInstance(), startHour, startMinute, stopHour, stopMinute);
  }

  // for unit tests
  boolean canRunTime(Calendar now){
      return canRunTime(now, startHour, startMinute, stopHour, stopMinute);
  }

  private boolean canRunTime(Calendar now, int startHour, int startMinute, int stopHour, int stopMinute){
    if(!limitRunningTime){
      return true;
    }
    int nowHour = now.get(Calendar.HOUR_OF_DAY);
    int nowMinute = now.get(Calendar.MINUTE);
    if(startHour < stopHour || ((startHour == stopHour) && (startMinute < stopMinute))){
      // normal range
      if(nowHour < startHour){
        return false;
      }
      if(nowHour > stopHour){
        return false;
      }
      if(nowHour == startHour && nowMinute < startMinute){
        return false;
      }
      if(nowHour == stopHour && nowMinute > stopMinute){
        return false;
      }
      if(nowHour < startHour){
        return false;
      }
      return true;
    }
    else{
      // outside range (durations over midnight)
      return !canRunTime(now, stopHour, stopMinute, startHour, startMinute);
    }
  }
  
  @Override
  public void start(){
  	if(BaseWarcDomainManager.isDisableIndexing()){
  		throw new IllegalStateException("Cannot start because indexing is disabled.");
  	}
  	synchronized (startStopLock){
  		if(pStarting == true){
  			return;
  		}
    	finishedFinding = false;
    	pStarting = true;
    	super.start();			
		}
  }
  
  @Override
  public void startInnner(){
  	synchronized (startStopLock){  	
      	super.startInnner();
      	pStarting = false;
  	}
  }
  
  @Override
  public void stop(){
  	while(pStarting){ // start not finished so wait
  		try{
				Thread.sleep(1000);
			}
			catch (InterruptedException e){
				// ignore
			}
  	}
  	synchronized (startStopLock){
    	stopPressed = true;
    	finishedFinding = true;
    	stopInner();
    	stopPressed = false;
  	}
  }
  
  public void stopInner() {
  	synchronized (startStopLock){
  		if(!stopPressed && !pStopping){
  			// stop from parent ignore and react to stop from this domain
  			return;
  		}
  		pStopping = false;
  		if(pStarting){
  			return; // race condition detected let the start win
  		}
      boolean holdRunning = running;
      boolean holdStopping = stopping;
  		super.stopInner();
  		runMessage = "";
    	if(!stopPressed && holdRunning && !holdStopping){
    		// in periodic domain only stop when button pressed.
    		// not pressed so restart
    		runMessage = " recheck for new content.";
    		try{
    			Thread.sleep(60000);
    		}
    		catch(InterruptedException e){
    			// ignore
    		}
    		start();
    	}
  	}
  }
  
  @Override
  public void run(){
    // we will stay here until we are in the time window to run
    while(true){
      boolean canRunNow = canRunTime();
      if(!canRunNow){
        // as we are not in the time window to run we reset the finished flag to allow the next run to start.  
        finishedFinding = false;
        runMessage = " restart at " + startHour + ":" + minuteTxt(startMinute);
      }
      if(canRunNow && !finishedFinding){
        if(!limitRunningTime){
          runMessage = ""; // can run at any time no extra message.
        }
        else{
          runMessage = " pause at " + stopHour + ":" + minuteTxt(stopMinute);
        }
        break;
      }
      try{
        Thread.sleep(60000);
      }
      catch (InterruptedException e){
        // ignore
      }
      if(!running){
        // some one must have pressed the stop button
        runMessage = "";
        return;
      }
    }
    super.run();
    runMessage = "";
  }
  private String minuteTxt(int minute){
    if(minute < 10){
     return "0" + minute;
    }
    return "" + minute;
  }

  @Override
  protected void loop(){
    // we have to stop when we go out side the time window to run
    while (canRunTime() && running && !stopping && !finishedFinding) {
      try {
        try {
          doWork();

        } catch (IOException e) {
          log.error("Error talking to Bamboo. Waiting 5 minutes before trying again: '{}'", e.getMessage());
          // Try again in 5 minutes
          Thread.sleep(5 * 60 * 1000);

        } catch (Exception e) {
          log.error("Unexpected error during doWork(). Waiting 1 hour before trying again: ", e);
          Thread.sleep(60 * 60 * 1000);
        }
      } catch (InterruptedException e) {
        log.error("Thread sleep interrupted whilst waiting on batch completion. Resuming: {}", e.getMessage());
      }
    }
  	synchronized (startStopLock){
  		pStopping = true;
  		finishedFinding = true;
  		stopInner();
  	}
  }
}
