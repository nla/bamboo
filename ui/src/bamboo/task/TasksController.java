package bamboo.task;

import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import bamboo.util.Csrf;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import spark.Request;
import spark.Response;
import spark.Spark;

import static bamboo.util.Freemarker.render;

public class TasksController {
    private final Warcs warcs;
    private final TaskDAO taskDAO;

    public void routes() {
        Spark.get("/tasks", this::index);
        Spark.get("/tasks/CdxIndexer/queue", this::cdxQueue);
        Spark.get("/tasks/SolrIndexer/queue", this::solrQueue);
        Spark.post("/tasks/:id/disable", this::disable);
        Spark.post("/tasks/:id/enable", this::enable);
    }

    public TasksController(Warcs warcs, TaskDAO taskDAO) {
        this.warcs = warcs;
        this.taskDAO = taskDAO;
    }

    String index(Request request, Response response) {
        return render(request, "bamboo/views/tasks.ftl",
                "csrfToken", Csrf.token(request),
                "tasks", taskDAO.listTasks());
    }

    String disable(Request request, Response response) {
        if (taskDAO.setEnabled(request.params(":id"), false) == 0 ) {
            throw Spark.halt(404, "No such task");
        }
        response.redirect(request.contextPath() + "/tasks", 303);
        return "";
    }

    String enable(Request request, Response response) {
        if (taskDAO.setEnabled(request.params(":id"), true) == 0 ) {
            throw Spark.halt(404, "No such task");
        }
        response.redirect(request.contextPath() + "/tasks", 303);
        return "";
    }

    String cdxQueue(Request request, Response response) {
        Pager<Warc> pager = warcs.paginateWithState(Parsing.parseLongOrDefault(request.queryParams("page"), 1), Warc.IMPORTED);
        return render(request, "bamboo/views/tasks/warcs.ftl",
                "queueName", "CDX Indexing",
                "warcs", pager.items,
                "warcsPager", pager);
    }

    String solrQueue(Request request, Response response) {
        Pager<Warc> pager = warcs.paginateWithState(Parsing.parseLongOrDefault(request.queryParams("page"), 1), Warc.CDX_INDEXED);
        return render(request, "bamboo/views/tasks/warcs.ftl",
                "queueName", "Solr Indexing",
                "warcs", pager.items,
                "warcsPager", pager);
    }
}
