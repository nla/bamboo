package bamboo.trove.common;

import bamboo.crawl.Warc;

public class WarcToIndex {
  private long id;
  private long urlCount = 0;

  // For Jackson - client side
  public WarcToIndex() {
  }

  // For Bamboo - server side
  public WarcToIndex(Warc warc) {
    this.id = warc.getId();
    this.urlCount = warc.getRecords();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getUrlCount() {
    return urlCount;
  }

  public void setUrlCount(long urlCount) {
    this.urlCount = urlCount;
  }
}