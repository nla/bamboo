package bamboo.crawl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Warc {
    public final static int OPEN = 0, IMPORTED = 1, CDX_INDEXED = 2, SOLR_INDEXED = 3;
    public final static int IMPORT_ERROR = -1, CDX_ERROR = -2, SOLR_ERROR = -3;

    private long id;
    private long crawlId;
    private int stateId;
    private Path path;
    private long size;
    private long records;
    private long recordBytes;
    private String filename;
    private String sha256;

	public Warc() {
	}

    public Warc(ResultSet rs) throws SQLException {
        setId(rs.getLong("id"));
        setCrawlId(rs.getLong("crawl_id"));
        setStateId(rs.getInt("warc_state_id"));
        setPath(Paths.get(rs.getString("path")));
        setSize(rs.getLong("size"));
        setRecords(rs.getLong("records"));
        setRecordBytes(rs.getLong("record_bytes"));
        setFilename(rs.getString("filename"));
        setSha256(rs.getString("sha256"));
    }

	public static Warc fromFile(Path path) throws IOException {
        Warc warc = new Warc();
        warc.setPath(path);
		warc.setFilename(path.getFileName().toString());
        warc.setSize(Files.size(path));
        warc.setSha256(Scrub.calculateDigest("SHA-256", path));
        return warc;
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

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public long getSize() {
		return size;
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
}
