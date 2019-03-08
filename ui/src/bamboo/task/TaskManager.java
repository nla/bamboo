package bamboo.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TaskManager implements Runnable, AutoCloseable {
    private final Logger log  = LoggerFactory.getLogger(getClass());
    private final TaskDAO dao;
    private final Map<String, Runnable> runnables = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> futures = new HashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    public TaskManager(TaskDAO dao) {
        this.dao = dao;
    }

    public void register(Runnable runnable) {
        Task task = new Task(runnable);
        dao.register(task);
        runnables.put(task.getId(), runnable);
    }

    public void run() {
        for (Task task : dao.listTasks()) {
            if (task.isEnabled()) {
                Runnable runnable = runnables.get(task.getId());
                if (runnable == null) continue;
                futures.computeIfAbsent(task.getId(), (id) -> scheduler.scheduleWithFixedDelay(wrap(id, runnable), 0, 1, SECONDS));
            } else if (!task.isEnabled()) {
                Future<?> future = futures.remove(task.getId());
                if (future != null) {
                    future.cancel(true);
                }
            }
        }
    }

    private Runnable wrap(String id, Runnable runnable) {
        return () -> {
            try {
                try {
                    dao.setStartTime(id, Timestamp.from(Instant.now()));
                    runnable.run();
                } finally {
                    dao.setFinishTime(id, Timestamp.from(Instant.now()));
                }
            } catch (Throwable e) {
                log.error(id + ": " + e.getMessage(), e);
            }
        };
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this, 0, 1, SECONDS);
    }

    public void close() {
        scheduler.shutdownNow();
    }
}
