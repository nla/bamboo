package bamboo.crawl;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Artifact {
    private final long id;
    private final String relpath;
    private final long size;
    private final String type;
    private final String sha256;
    private final Long blobId;

    public Artifact(ResultSet rs) throws SQLException {
        id = rs.getLong("id");
        relpath = rs.getString("relpath");
        size = rs.getLong("size");
        type = rs.getString("type");
        sha256 = rs.getString("sha256");
        blobId = (Long) rs.getObject("blob_id");
    }

    public long getId() {
        return id;
    }

    public String getRelpath() {
        return relpath;
    }

    public long getSize() {
        return size;
    }

    public String getType() {
        return type;
    }

    public String getSha256() {
        return sha256;
    }

    public Long getBlobId() {
        return blobId;
    }

    public String guessContentType() {
        if (relpath == null) return "application/octet-stream";
        if (relpath.endsWith(".cxml")) return "application/xml";
        if (relpath.endsWith(".log")) return "text/plain";
        if (relpath.endsWith(".dump")) return "text/plain";
        if (relpath.endsWith(".txt")) return "text/plain";
        return "application/octet-stream";
    }
}