package bamboo.crawl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class Series {
    private long id;
    private String name;
    private Path path;
    private long warcFiles;
    private long warcSize;
    private long records;
    private long recordBytes;
    private String description;
    private String creator;
    private Date created;
    private String modifier;
    private Date modified;
    private Integer agencyId;

    public Series() {
    }

    public Series(ResultSet rs) throws SQLException {
        setId(rs.getLong("id"));
        setName(rs.getString("name"));
        setPath(maybePath(rs.getString("path")));
        setWarcFiles(rs.getLong("warc_files"));
        setWarcSize(rs.getLong("warc_size"));
        setRecords(rs.getLong("records"));
        setRecordBytes(rs.getLong("record_bytes"));
        setDescription(rs.getString("description"));
        setCreator(rs.getString("creator"));
        setCreated(rs.getDate("created"));
        setModifier(rs.getString("modifier"));
        setModified(rs.getDate("modified"));
        setAgencyId(rs.getInt("agency_id"));
        if (rs.wasNull()) {
            setAgencyId(null);
        }
    }

    private static Path maybePath(String path) {
        return path == null ? null : Paths.get(path);
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

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public long getWarcFiles() {
        return warcFiles;
    }

    public void setWarcFiles(long warcFiles) {
        this.warcFiles = warcFiles;
    }

    public long getWarcSize() {
        return warcSize;
    }

    public void setWarcSize(long warcSize) {
        this.warcSize = warcSize;
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

    public String getCreator() {
        return creator;
    }

    public Series setCreator(String creator) {
        this.creator = creator;
        return this;
    }

    public Date getCreated() {
        return created;
    }

    public Series setCreated(Date created) {
        this.created = created;
        return this;
    }

    public String getModifier() {
        return modifier;
    }

    public Series setModifier(String modifier) {
        this.modifier = modifier;
        return this;
    }

    public Date getModified() {
        return modified;
    }

    public Series setModified(Date modified) {
        this.modified = modified;
        return this;
    }

    public Integer getAgencyId() {
        return agencyId;
    }

    public void setAgencyId(Integer agencyId) {
        this.agencyId = agencyId;
    }
}
