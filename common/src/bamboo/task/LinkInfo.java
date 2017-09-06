package bamboo.task;

public class LinkInfo {
    private String text;
    private String type;
    private String url;
    private String rel;
    private String title;
    private String href;

    public void setText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public void setRel(String rel) {
        this.rel = rel;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public String getRel() {
        return rel;
    }

    public void setHref(String href) {
        this.href = href;
    }

    public String getHref() {
        return href;
    }
}
