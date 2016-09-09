package bamboo.crawl;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CollectionWarc {
    public final long collectionId;
    public final long warcId;
    public final long records;
    public final long recordBytes;

    public CollectionWarc(ResultSet rs) throws SQLException {
        collectionId = rs.getLong("collection_id");
        warcId = rs.getLong("warc_id");
        records = rs.getLong("records");
        recordBytes = rs.getLong("record_bytes");
    }
}
