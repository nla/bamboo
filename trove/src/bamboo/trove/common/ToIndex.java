package bamboo.trove.common;

import bamboo.task.WarcToIndex;

public class ToIndex extends WarcToIndex {
  public boolean hasBeenRetrieved = false;
  private WarcProgressManager oldWarcInstance = null;
  private long trackedOffset = -1;

  // A normal event from Bamboo
  public ToIndex(WarcToIndex warc) {
    setId(warc.getId());
    setUrlCount(warc.getUrlCount());
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
