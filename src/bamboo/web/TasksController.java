package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import droute.Handler;
import droute.Request;
import droute.Response;

import static droute.Response.notFound;
import static droute.Response.render;
import static droute.Route.GET;
import static droute.Route.routes;

public class TasksController {
    final Bamboo bamboo;
    public Handler routes = routes(
            GET("/tasks", this::index));

    public TasksController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        return render("crawls/index.ftl", "tasks", bamboo.taskmaster.getJobs());
    }

}