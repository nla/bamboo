package bamboo.virus;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

public class VirusScanRun {
    private final long id;
    private final String state;
    private final long maxWarcId;
    private final long lastWarcId;
    private final long warcsTotal;
    private final Instant finishedAt;

    public VirusScanRun(ResultSet rs) throws SQLException {
        id = rs.getLong("id");
        state = rs.getString("state");
        maxWarcId = rs.getLong("max_warc_id");
        lastWarcId = rs.getLong("last_warc_id");
        warcsTotal = rs.getLong("warcs_total");
        Timestamp timestamp = rs.getTimestamp("finished_at");
        finishedAt = timestamp == null ? null : timestamp.toInstant();
    }

    public long getId() {
        return id;
    }

    public String getState() {
        return state;
    }

    public long getMaxWarcId() {
        return maxWarcId;
    }

    public long getLastWarcId() {
        return lastWarcId;
    }

    public long getWarcsTotal() {
        return warcsTotal;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }
}

