package bamboo.task;

import bamboo.core.Config;
import bamboo.core.DbPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Taskmaster {
    final Task importer;
    final List<Task> indexers = new ArrayList<>();
    final List<Task> tasks = new ArrayList<>();

    public Taskmaster(Config config, DbPool dbPool) {
        importer = new Task(new Importer(config, dbPool, this::startIndexing));
        tasks.add(importer);
        indexers.add(new Task(new CdxIndexer(dbPool)));
        indexers.add(new Task(new SolrIndexer(dbPool)));
        tasks.addAll(indexers);
    }

    public void startIndexing() {
        for (Task task: indexers) {
            task.start();
        }
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

    public void startImporting() {
        importer.start();
    }
}
