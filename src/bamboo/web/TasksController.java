package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.task.Task;
import bamboo.util.Pager;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import static droute.Response.notFound;
import static droute.Response.render;
import static droute.Response.seeOther;
import static droute.Route.*;

class TasksController {
    final Bamboo bamboo;
    final Handler routes = routes(
            GET("/tasks", this::index),
            GET("/tasks/CdxIndexer/queue", this::cdxQueue),
            GET("/tasks/SolrIndexer/queue", this::solrQueue),
            POST("/tasks/:id/disable", this::disable),
            POST("/tasks/:id/enable", this::enable));

    TasksController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        return render("tasks.ftl",
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
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.Warc> pager = new Pager<>(request, "page", db.countWarcsToBeCdxIndexed(),
                    db::paginateWarcsToBeCdxIndexed);
            return render("tasks/warcs.ftl",
                    "queueName", "CDX Indexing",
                    "warcs", pager.items,
                    "warcsPager", pager);
        }
    }

    Response solrQueue(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.Warc> pager = new Pager<>(request, "page", db.countWarcsToBeSolrIndexed(),
                    db::paginateWarcsToBeSolrIndexed);
            return render("tasks/warcs.ftl",
                    "queueName", "Solr Indexing",
                    "warcs", pager.items,
                    "warcsPager", pager);
        }
    }
}
