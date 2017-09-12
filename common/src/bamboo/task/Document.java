package bamboo.task;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Date;
import java.util.List;

public class Document {
    private String url;
    private String deliveryUrl;
    private String pandoraUrl;
    private Date date;
    private int statusCode;
    private long contentLength;
    private String contentType;
    private String title;
    private String text;

    /**
     * If non-null this indicates that text extraction from this record failed and explains why.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String textError;

    private String boiled;
    private String contentSha1;
    private String site;
    private long warcOffset;
    private String host;
    private String description;
    private String keywords;
    private String publisher;
    private String creator;
    private String contributor;
    private String coverage;
    private List<LinkInfo> links;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDeliveryUrl() {
        return deliveryUrl;
    }

    public void setDeliveryUrl(String deliveryUrl) {
        this.deliveryUrl = deliveryUrl;
    }

    public String getPandoraUrl() {
        return pandoraUrl;
    }

    public void setPandoraUrl(String pandoraUrl) {
        this.pandoraUrl = pandoraUrl;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getBoiled() {
        return boiled;
    }

    public void setBoiled(String boiled) {
        this.boiled = boiled;
    }

    public void setContentSha1(String contentSha1) {
        this.contentSha1 = contentSha1;
    }

    public String getSha1() {
        return contentSha1;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getSite() {
        return site;
    }

    public void setWarcOffset(long warcOffset) {
        this.warcOffset = warcOffset;
    }

    public long getWarcOffset() {
        return warcOffset;
    }

    public String getTextError() {
        return textError;
    }

    public void setTextError(String textError) {
        this.textError = textError;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCreator() {
        return creator;
    }

    public void setContributor(String contributor) {
        this.contributor = contributor;
    }

    public String getContributor() {
        return contributor;
    }

    public void setCoverage(String coverage) {
        this.coverage = coverage;
    }

    public String getCoverage() {
        return coverage;
    }

    public void setLinks(List<LinkInfo> links) {
        this.links = links;
    }

    public List<LinkInfo> getLinks() {
        return links;
    }
}
