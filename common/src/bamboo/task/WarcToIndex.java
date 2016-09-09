package bamboo.task;

public class WarcToIndex {
  private long id;
  private long urlCount = 0;

  // For Jackson - client side
  public WarcToIndex() {
  }

  // For Bamboo - server side
  public WarcToIndex(long id, long urlCount) {
    this.id = id;
    this.urlCount = urlCount;
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