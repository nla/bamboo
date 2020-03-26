package bamboo.crawl;

import bamboo.core.Permission;
import org.hibernate.annotations.Formula;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;

@SuppressWarnings("JpaAttributeTypeInspection")
@Entity
@Table(name = "crawl_series")
public class Series {
    @Id @GeneratedValue(strategy= GenerationType.IDENTITY) private Long id;
    @NotNull private String name;
    private Path path;
    private long warcFiles;
    private long warcSize;
    private long records;
    private long recordBytes;
    private String description;
    @CreatedBy private String creator;
    @CreatedDate private Date created;
    @LastModifiedBy private String modifier;
    @LastModifiedDate private Date modified;
    @Column(name="agency_id") private Integer agencyId;
    @ManyToOne  @JoinColumn(name = "agency_id", insertable = false, updatable = false) private Agency agency;

    @Formula("(select count(*) from crawl where crawl.crawl_series_id = id)")
    private Long crawlCount;

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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
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

    public Agency getAgency() {
        return agency;
    }

    public Long getCrawlCount() {
        return crawlCount;
    }
}
