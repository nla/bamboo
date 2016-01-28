package bamboo.task;

import bamboo.app.Bamboo;
import bamboo.crawl.Warc;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import static droute.Response.notFound;
import static droute.Response.render;
import static droute.Response.seeOther;
import static droute.Route.*;

public class TasksController {
    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/tasks", this::index),
            GET("/tasks/CdxIndexer/queue", this::cdxQueue),
            GET("/tasks/SolrIndexer/queue", this::solrQueue),
            POST("/tasks/:id/disable", this::disable),
            POST("/tasks/:id/enable", this::enable));

    public TasksController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        return render("bamboo/views/tasks.ftl",
                "csrfToken", Csrf.token(request),
                "tasks", bamboo.taskmaster.getTasks());
    }

    Response disable(Request request) {
        Task task = bamboo.taskmaster.find(request.param("id"));
        if (task == null) {
            return notFound("No such task");
        }
        task.disable();
        return seeOther(request.contextUri().resolve("tasks").toString());
    }

    Response enable(Request request) {
        Task task = bamboo.taskmaster.find(request.param("id"));
        if (task == null) {
            return notFound("No such task");
        }
        task.enable();
        return seeOther(request.contextUri().resolve("tasks").toString());
    }

    Response cdxQueue(Request request) {
        Pager<Warc> pager = bamboo.warcs.paginateWithState(Parsing.parseLongOrDefault(request.queryParam("page"), 1), Warc.IMPORTED);
        return render("bamboo/views/tasks/warcs.ftl",
                "queueName", "CDX Indexing",
                "warcs", pager.items,
                "warcsPager", pager);
    }

    Response solrQueue(Request request) {
        Pager<Warc> pager = bamboo.warcs.paginateWithState(Parsing.parseLongOrDefault(request.queryParam("page"), 1), Warc.CDX_INDEXED);
        return render("bamboo/views/tasks/warcs.ftl",
                "queueName", "Solr Indexing",
                "warcs", pager.items,
                "warcsPager", pager);
    }
}
