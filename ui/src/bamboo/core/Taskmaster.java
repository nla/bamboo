package bamboo.core;

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

    public void startAll() {
        for (Task task : tasks) {
            task.enable();
        }
    }
}
