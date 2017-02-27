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

import bamboo.task.WarcToIndex;
import bamboo.task.WarcToIndexResumption;

public class ToIndex extends WarcToIndexResumption {
  public boolean hasBeenRetrieved = false;
  private WarcProgressManager oldWarcInstance = null;
  private long trackedOffset = -1;

  // A normal event from Bamboo
  public ToIndex(WarcToIndex warc) {
    setId(warc.getId());
    setUrlCount(warc.getUrlCount());
    if(warc instanceof WarcToIndexResumption){
      setResumptionToken(((WarcToIndexResumption)warc).getResumptionToken());
    }
  }

  // Something from the retry queue
  public ToIndex(WarcProgressManager warc) {
    setId(warc.getWarcId());
    setUrlCount(warc.getUrlCountEstimate());
    oldWarcInstance = warc;
  }

  public boolean isRetryAttempt() {
    return !(oldWarcInstance == null);
  }

  public WarcProgressManager oldAttempt() {
    return oldWarcInstance;
  }

  public long getTrackedOffset() {
    return trackedOffset;
  }

  public void setTrackedOffset(long trackedOffset) {
    this.trackedOffset = trackedOffset;
  }
}
