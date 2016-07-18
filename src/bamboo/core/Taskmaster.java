package bamboo.core;

import bamboo.core.Config;
import bamboo.core.DbPool;
import bamboo.task.CdxIndexer;
import bamboo.task.Importer;
import bamboo.task.SolrIndexer;
import bamboo.task.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Taskmaster {
    final List<Task> tasks = new ArrayList<>();

    public void add(Runnable runnable) {
        tasks.add(new Task(runnable));
    }

    public List<Task> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public Task find(String id) {
        for (Task task : tasks) {
            if (id.equals(task.getId())) {
                return task;
            }
        }
        return null;
    }

    public void close() {
        for (Task task : tasks) {
            task.disable();
        }
    }
}
