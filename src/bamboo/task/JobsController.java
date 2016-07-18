package bamboo.task;

import bamboo.app.Bamboo;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;
import org.apache.commons.io.FileUtils;

import java.io.IOException;

import static droute.Response.*;
import static droute.Route.*;

public class JobsController {
    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/jobs", this::index),
            POST("/jobs/:job/delete", this::delete));

    public JobsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response delete(Request request) {
        String jobName = request.urlParam("job");
        HeritrixJob job = HeritrixJob.byName(bamboo.config.getHeritrixJobs(), jobName);
        try {
            FileUtils.deleteDirectory(job.dir().toFile());
        } catch (IOException e) {
            e.printStackTrace();
            return response(500, e.toString());
        }
        return seeOther(request.contextUri().resolve("jobs").toString());
    }

    Response index(Request request) {
        return render("bamboo/views/jobs/index.ftl",
                "csrfToken", Csrf.token(request),
                "heritrixUrl", bamboo.config.getHeritrixUrl(),
                "jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()));
    }
}