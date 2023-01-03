package bamboo.task;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@RegisterRowMapper(TaskDAO.TaskMapper.class)
public interface TaskDAO {
    @SqlUpdate("INSERT INTO task (id, name) VALUES (:id, :name) ON DUPLICATE KEY UPDATE name = :name")
    void register(@BindBean Task task);

    @SqlQuery("SELECT * FROM task ORDER BY id ASC")
    List<Task> listTasks();

    @SqlUpdate("UPDATE task SET enabled = :enabled WHERE id = :id")
    int setEnabled(@Bind("id") String id, @Bind("enabled") boolean enabled);

    @SqlUpdate("UPDATE task SET start_time = :startTime, finish_time = NULL WHERE id = :id")
    int setStartTime(@Bind("id") String id, @Bind("startTime") Timestamp startTime);

    @SqlUpdate("UPDATE task SET finish_time = :finishTime WHERE id = :id")
    int setFinishTime(@Bind("id") String id, @Bind("finishTime") Timestamp finishTime);

    class TaskMapper implements RowMapper<Task> {
        @Override
        public Task map(ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Task(resultSet);
        }
    }
}
