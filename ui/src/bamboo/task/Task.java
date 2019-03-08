package bamboo.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.SECONDS;

public class Task {
    private final String id;
    private final String name;
    private final boolean enabled;
    private final Instant startTime;
    private final Instant finishTime;

    public Task(ResultSet resultSet) throws SQLException {
        id = resultSet.getString("id");
        name = resultSet.getString("name");
        enabled = resultSet.getBoolean("enabled");
        startTime = instant(resultSet.getTimestamp("start_time"));
        finishTime = instant(resultSet.getTimestamp("finish_time"));
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    public Task(Runnable runnable) {
        id = runnable.getClass().getSimpleName();
        name = id.replaceAll("([a-z])([A-Z])", "$1 $2");
        enabled = true;
        startTime = null;
        finishTime = null;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getStatus() {
        if (startTime != null && finishTime == null) {
            return "Running" + duration();
        }
        return enabled ? "Idle" + duration() : "Disabled" + duration();
    }

    private String duration() {
        if (startTime == null) return "";
        Duration duration = Duration.between(startTime.truncatedTo(SECONDS), Instant.now().truncatedTo(SECONDS));
        if (duration.toMillis() < 3000) return "";
        return " (" + duration
                .toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase() + ")";
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }
}
