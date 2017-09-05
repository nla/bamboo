package bamboo.task;

import bamboo.app.Bamboo;
import bamboo.util.Csrf;
import org.apache.commons.io.FileUtils;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.IOException;

import static bamboo.util.Freemarker.render;

public class JobsController {
    final Bamboo bamboo;

    public void routes() {
        Spark.get("/jobs", this::index);
        Spark.post("/jobs/:job/delete", this::delete);
    }

    public JobsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    String delete(Request request, Response response) throws IOException {
        String jobName = request.params(":job");
        HeritrixJob job = HeritrixJob.byName(bamboo.config.getHeritrixJobs(), jobName);
        FileUtils.deleteDirectory(job.dir().toFile());
        response.redirect(request.contextPath() + "/jobs", 303);
        return "";
    }

    String index(Request request, Response response) {
        return render(request, "bamboo/views/jobs/index.ftl",
                "csrfToken", Csrf.token(request),
                "heritrixUrl", bamboo.config.getHeritrixUrl(),
                "jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()));
    }
}