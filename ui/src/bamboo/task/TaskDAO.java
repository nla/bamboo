package bamboo.task;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@RegisterMapper(TaskDAO.TaskMapper.class)
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

    class TaskMapper implements ResultSetMapper<Task> {
        @Override
        public Task map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Task(resultSet);
        }
    }
}
