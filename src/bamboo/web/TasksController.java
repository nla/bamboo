package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.task.Task;
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
}
