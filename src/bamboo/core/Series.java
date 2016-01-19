package bamboo.core;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Series {
    private long id;
    private String name;
    private Path path;
    private long warcFiles;
    private long warcSize;
    private long records;
    private long recordBytes;
    private String description;

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
}
