package bamboo.trove.common;

public enum SolrEnum {
  ID("id"),
  URL("url"),
  DATE("date"),
  TITLE("title"),
  CONTENT_TYPE("contentType"),
  SITE("site"),
  RESTRICTED("restricted");

  private String value;
  SolrEnum(String value) {
    this.value = value;
  }
  public String toString() {
    return value;
  }
}