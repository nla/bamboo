package bamboo.crawl;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Objects;

@SuppressWarnings("JpaAttributeTypeInspection")
@Entity
public class Crawl {
	public static final int ARCHIVED = 0;
	public static final int IMPORTING = 1;
	public static final int IMPORT_FAILED = 2;
	@Id private long id;
    @NotBlank private String name;
    private Long totalDocs;
    private Long totalBytes;
	@NotNull private Long crawlSeriesId;
    private Path path;
    private int state;
    private long warcFiles;
    private long warcSize;
    private long records;
    private long recordBytes;
    private String description;
    private Date startTime;
    private Date endTime;
    private Long pandasInstanceId;
	private String creator;
	private Date created;
	private String modifier;
	private Date modified;
	private String webrecorderCollectionId;

	public Crawl() {
	}

	public Crawl(Crawl crawl) {
		this.id = crawl.getId();
		this.name = crawl.getName();
		this.totalDocs = crawl.getTotalDocs();
		this.totalBytes = crawl.getTotalBytes();
		this.crawlSeriesId = crawl.getCrawlSeriesId();
		this.path = crawl.getPath();
		this.state = crawl.getState();
		this.warcFiles = crawl.getWarcFiles();
		this.warcSize = crawl.getWarcSize();
		this.records = crawl.getRecords();
		this.recordBytes = crawl.getRecordBytes();
		this.description = crawl.getDescription();
		this.startTime = crawl.getStartTime();
		this.endTime = crawl.getEndTime();
		this.pandasInstanceId = crawl.getPandasInstanceId();
		this.creator = crawl.getCreator();
		this.created = crawl.getCreated();
		this.modifier = crawl.getModifier();
		this.modified = crawl.getModified();
		this.webrecorderCollectionId = crawl.getWebrecorderCollectionId();
	}

	public Crawl(ResultSet rs) throws SQLException {
        String path = rs.getString("path");
        Integer state = (Integer)rs.getObject("state");
        setId(rs.getLong("id"));
        setName(rs.getString("name"));
        setTotalDocs((Long)rs.getObject("total_docs"));
        setTotalBytes((Long)rs.getObject("total_bytes"));
        setCrawlSeriesId((Long)rs.getObject("crawl_series_id"));
        this.setPath(path != null ? Paths.get(path) : null);
        this.setState(state != null ? state : 0);
        setWarcFiles(rs.getLong("warc_files"));
        setWarcSize(rs.getLong("warc_size"));
        setRecords(rs.getLong("records"));
        setRecordBytes(rs.getLong("record_bytes"));
        setDescription(rs.getString("description"));
        setStartTime((Date)rs.getObject("start_time"));
        setEndTime((Date)rs.getObject("end_time"));
        setPandasInstanceId((Long)rs.getObject("pandas_instance_id"));
		setCreator(rs.getString("creator"));
		setCreated(rs.getDate("created"));
		setModifier(rs.getString("modifier"));
		setModified(rs.getDate("modified"));
		setWebrecorderCollectionId(rs.getString("webrecorder_collection_id"));
    }

    private static final String[] STATE_NAMES = {"Archived", "Importing", "Import Failed"};

    public String stateName() {
        return STATE_NAMES[getState()];
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

	public Long getTotalDocs() {
		return totalDocs;
	}

	public void setTotalDocs(Long totalDocs) {
		this.totalDocs = totalDocs;
	}

	public Long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(Long totalBytes) {
		this.totalBytes = totalBytes;
	}

	public Long getCrawlSeriesId() {
		return crawlSeriesId;
	}

	public void setCrawlSeriesId(Long crawlSeriesId) {
		this.crawlSeriesId = crawlSeriesId;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
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

	public Long getPandasInstanceId() {
		return pandasInstanceId;
	}

	public void setPandasInstanceId(Long pandasInstanceId) {
		this.pandasInstanceId = pandasInstanceId;
	}

	public String getCreator() {
		return creator;
	}

	public Crawl setCreator(String creator) {
		this.creator = creator;
		return this;
	}

	public Date getCreated() {
		return created;
	}

	public Crawl setCreated(Date created) {
		this.created = created;
		return this;
	}

	public String getModifier() {
		return modifier;
	}

	public Crawl setModifier(String modifier) {
		this.modifier = modifier;
		return this;
	}

	public Date getModified() {
		return modified;
	}

	public Crawl setModified(Date modified) {
		this.modified = modified;
		return this;
	}

	public String getWebrecorderCollectionId() {
		return webrecorderCollectionId;
	}

	public void setWebrecorderCollectionId(String webrecorderCollectionId) {
		this.webrecorderCollectionId = webrecorderCollectionId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Crawl crawl = (Crawl) o;
		return id == crawl.id &&
				state == crawl.state &&
				warcFiles == crawl.warcFiles &&
				warcSize == crawl.warcSize &&
				records == crawl.records &&
				recordBytes == crawl.recordBytes &&
				Objects.equals(name, crawl.name) &&
				Objects.equals(totalDocs, crawl.totalDocs) &&
				Objects.equals(totalBytes, crawl.totalBytes) &&
				Objects.equals(crawlSeriesId, crawl.crawlSeriesId) &&
				Objects.equals(path, crawl.path) &&
				Objects.equals(description, crawl.description) &&
				Objects.equals(startTime, crawl.startTime) &&
				Objects.equals(endTime, crawl.endTime) &&
				Objects.equals(pandasInstanceId, crawl.pandasInstanceId) &&
				Objects.equals(creator, crawl.creator) &&
				Objects.equals(created, crawl.created) &&
				Objects.equals(modifier, crawl.modifier) &&
				Objects.equals(modified, crawl.modified) &&
				Objects.equals(webrecorderCollectionId, crawl.webrecorderCollectionId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, name, totalDocs, totalBytes, crawlSeriesId, path, state, warcFiles, warcSize, records, recordBytes, description, startTime, endTime, pandasInstanceId, creator, created, modifier, modified, webrecorderCollectionId);
	}
}
