package bamboo.task;

import bamboo.app.Bamboo;
import bamboo.crawl.Warc;
import bamboo.util.Csrf;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import spark.Spark;
import spark.Request;
import spark.Response;

import static bamboo.util.Freemarker.render;

public class TasksController {
    final Bamboo bamboo;

    public void routes() {
        Spark.get("/tasks", this::index);
        Spark.get("/tasks/CdxIndexer/queue", this::cdxQueue);
        Spark.get("/tasks/SolrIndexer/queue", this::solrQueue);
        Spark.post("/tasks/:id/disable", this::disable);
        Spark.post("/tasks/:id/enable", this::enable);
    }

    public TasksController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    String index(Request request, Response response) {
        return render(request, "bamboo/views/tasks.ftl",
                "csrfToken", Csrf.token(request),
                "tasks", bamboo.taskmaster.getTasks());
    }

    String disable(Request request, Response response) {
        Task task = bamboo.taskmaster.find(request.params(":id"));
        if (task == null) {
            throw Spark.halt(404, "No such task");
        }
        task.disable();
        response.redirect(request.contextPath() + "/tasks", 303);
        return "";
    }

    String enable(Request request, Response response) {
        Task task = bamboo.taskmaster.find(request.params(":id"));
        if (task == null) {
            throw Spark.halt(404, "No such task");
        }
        task.enable();
        response.redirect(request.contextPath() + "/tasks", 303);
        return "";
    }

    String cdxQueue(Request request, Response response) {
        Pager<Warc> pager = bamboo.warcs.paginateWithState(Parsing.parseLongOrDefault(request.queryParams("page"), 1), Warc.IMPORTED);
        return render(request, "bamboo/views/tasks/warcs.ftl",
                "queueName", "CDX Indexing",
                "warcs", pager.items,
                "warcsPager", pager);
    }

    String solrQueue(Request request, Response response) {
        Pager<Warc> pager = bamboo.warcs.paginateWithState(Parsing.parseLongOrDefault(request.queryParams("page"), 1), Warc.CDX_INDEXED);
        return render(request, "bamboo/views/tasks/warcs.ftl",
                "queueName", "Solr Indexing",
                "warcs", pager.items,
                "warcsPager", pager);
    }
}
