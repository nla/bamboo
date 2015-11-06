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

        Task solrIndexerTask = new Task(new SolrIndexer(dbPool));

        CdxIndexer cdxIndexer = new CdxIndexer(dbPool);
        cdxIndexer.onWarcIndexed(warcId -> solrIndexerTask.start());

        indexers.add(new Task(cdxIndexer));
        indexers.add(solrIndexerTask);

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
