package bamboo.crawl;

import doss.Blob;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Artifact {
    private final Long id;
    private final String relpath;
    private final long size;
    private final String type;
    private final String sha256;
    private final Long blobId;
    private final String path;

    public Artifact(ResultSet rs) throws SQLException {
        id = rs.getLong("id");
        relpath = rs.getString("relpath");
        size = rs.getLong("size");
        type = rs.getString("type");
        sha256 = rs.getString("sha256");
        blobId = (Long) rs.getObject("blob_id");
        path = rs.getString("path");
    }

    public Artifact(Blob blob, String relpath) throws IOException {
        id = null;
        this.relpath = relpath;
        type = typeFromFilename(relpath);
        blobId = blob.id();
        size = blob.size();
        path = null;
        try {
            sha256 = blob.digest("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    public Long getId() {
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

    public Path getPath() {
        return path == null ? null : Paths.get(path);
    }

    public String guessContentType() {
        if (relpath == null) return "application/octet-stream";
        if (relpath.endsWith(".cxml")) return "application/xml";
        if (relpath.endsWith(".log")) return "text/plain";
        if (relpath.endsWith(".dump")) return "text/plain";
        if (relpath.endsWith(".txt")) return "text/plain";
        return "application/octet-stream";
    }

    private static String typeFromFilename(String filename) {
        if (filename.endsWith(".log")) return "LOG";
        if (filename.endsWith(".xcxml")) return "CONFIG";
        if (filename.endsWith(".recover.gz")) return "RECOVER";
        if (filename.endsWith(".dump")) return "DUMP";
        return "UNKNOWN";
    }

}