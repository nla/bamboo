package bamboo.crawl;

import bamboo.util.Units;
import org.apache.commons.io.FileUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Collection {
    private long id;
    private String name;
    private String cdxUrl;
    private long records;
    private long recordBytes;
    private String description;

    public Collection() {
    }

    public Collection(ResultSet rs) throws SQLException {
        setId(rs.getLong("id"));
        setName(rs.getString("name"));
        setCdxUrl(rs.getString("cdx_url"));
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

    public String getRecordBytesDisplay() {
        return Units.displaySize(getRecordBytes());
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
