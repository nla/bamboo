package bamboo.crawl;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Collection {
    private long id;
    private String name;
    private String cdxUrl;
    private String solrUrl;
    private long records;
    private long recordBytes;
    private String description;

    public Collection() {
    }

    public Collection(ResultSet rs) throws SQLException {
        setId(rs.getLong("id"));
        setName(rs.getString("name"));
        setCdxUrl(rs.getString("cdx_url"));
        setSolrUrl(rs.getString("solr_url"));
        setRecords(rs.getLong("records"));
        setRecordBytes(rs.getLong("record_bytes"));
        setDescription(rs.getString("description"));
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCdxUrl() {
        return cdxUrl;
    }

    public void setCdxUrl(String cdxUrl) {
        this.cdxUrl = cdxUrl;
    }

    public String getSolrUrl() {
        return solrUrl;
    }

    public void setSolrUrl(String solrUrl) {
        this.solrUrl = solrUrl;
    }

    public long getRecords() {
        return records;
    }

    public void setRecords(long records) {
        this.records = records;
    }

    public long getRecordBytes() {
        return recordBytes;
    }

    public void setRecordBytes(long recordBytes) {
        this.recordBytes = recordBytes;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
