package bamboo.crawl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class Crawl {
    private long id;
    private String name;
    private Long totalDocs;
    private Long totalBytes;
    private Long crawlSeriesId;
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

	public Crawl() {
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
}
