package bamboo.crawl;

import org.apache.commons.io.FileUtils;

import jakarta.persistence.*;
import java.nio.file.Path;
import java.util.Date;

@SuppressWarnings("JpaAttributeTypeInspection")
@Entity
public class Warc {
    public final static int OPEN = 0, IMPORTED = 1, CDX_INDEXED = 2, SOLR_INDEXED = 3;
    public final static int IMPORT_ERROR = -1, CDX_ERROR = -2, SOLR_ERROR = -3, DELETED = -4;

    private @Id @GeneratedValue(strategy=GenerationType.IDENTITY) long id;
    private long crawlId;
    private @Column(name = "warc_state_id") int stateId;
    private Path path;
    private long size;
    private long records;
    private long recordBytes;
    private String filename;
    private String sha256;
    private Long blobId;
    private Date startTime;
    private Date endTime;
    private String software;

    public Warc() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getCrawlId() {
        return crawlId;
    }

    public void setCrawlId(long crawlId) {
        this.crawlId = crawlId;
    }

    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }

    public String getStateName() {
        return switch (stateId) {
            case Warc.DELETED -> "deleted";
            case Warc.SOLR_ERROR -> "solr_error";
            case Warc.CDX_ERROR -> "cdx_error";
            case Warc.IMPORT_ERROR -> "import_error";
            case Warc.OPEN -> "open";
            case Warc.IMPORTED -> "imported";
            case Warc.CDX_INDEXED -> "cdx_indexed";
            case Warc.SOLR_INDEXED -> "solr_indexed";
            default -> "unknown_" + stateId;
        };
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public String getDisplaySize() {
        return FileUtils.byteCountToDisplaySize(getSize()).replace("B", "iB");
    }

    public void setSize(long size) {
        this.size = size;
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

    public String getRecordBytesDisplay() {
        return FileUtils.byteCountToDisplaySize(getRecordBytes()).replace("B", "iB");
    }

    public void setRecordBytes(long recordBytes) {
        this.recordBytes = recordBytes;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getSha256() {
        return sha256;
    }

    public void setSha256(String sha256) {
        this.sha256 = sha256;
    }

    public void setBlobId(Long blobId) {
        this.blobId = blobId;
    }

    public Long getBlobId() {
        return blobId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getSoftware() {
        return software;
    }

    public void setSoftware(String software) {
        this.software = software;
    }
}
