package bamboo.app;

import bamboo.core.NotFoundException;
import bamboo.crawl.CollectionsController;
import bamboo.crawl.CrawlsController;
import bamboo.crawl.SeriesController;
import bamboo.crawl.WarcsController;
import bamboo.seedlist.SeedlistsController;
import bamboo.task.HeritrixJob;
import bamboo.task.JobsController;
import bamboo.task.TasksController;
import bamboo.util.Csrf;
import bamboo.util.Freemarker;
import org.apache.commons.lang.StringEscapeUtils;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import static bamboo.util.Parsing.parseLongOrDefault;

public class Webapp implements AutoCloseable {
    final Bamboo bamboo;

    public Webapp(Bamboo bamboo) {
        this.bamboo = bamboo;

        Spark.exception(NotFoundException.class, (e, request, response) -> {
            response.status(404);
            response.body("Not found: " + e.getMessage());
        });

        Spark.exception(Exception.class, (e, request, response) -> {
            response.status(500);
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String stackTrace = StringEscapeUtils.escapeHtml(sw.toString());
            response.body("<!doctype html><h1>Internal Server Error</h1><p>Please include the following text when reporting this error:</p><pre>"
                    + request.requestMethod() + " " + request.uri() + " " + new Date() + "\n"
                    + stackTrace + "</pre>");
        });;

        Spark.before(Csrf::protect);

        Spark.get("/", this::index);
        Spark.get("/import", this::showImportForm);
        Spark.post("/import", this::performImport);
        Spark.get("/healthcheck", this::healthcheck);
        Spark.get("/assets/*", this::serveStatic);
        Spark.get("/webjars/*", this::serveStatic);

        new CollectionsController(bamboo).routes();
        new CrawlsController(bamboo).routes();
        new JobsController(bamboo).routes();
        new SeedlistsController(bamboo).routes();
        new SeriesController(bamboo).routes();
        new TasksController(bamboo.warcs, bamboo.dao.tasks()).routes();
        new WarcsController(bamboo).routes();
    }

    private String healthcheck(Request request, Response response) {
        StringWriter out = new StringWriter();
        boolean ok = bamboo.healthcheck(new PrintWriter(out));
        response.status(ok ? 200 : 500);
        response.type("text/plain");
        return out.toString();
    }

    public Webapp() throws IOException {
        this(new Bamboo(System.getenv("NO_TASKS") != null));
    }

    String index(Request request, Response response) {
        return Freemarker.render(request, "bamboo/views/index.ftl",
                "seriesList", bamboo.serieses.listAll(),
                "collections", bamboo.collections.listAll());
    }

    String showImportForm(Request request, Response response) {
        return Freemarker.render(request, "bamboo/views/import.ftl",
                "allCrawlSeries", bamboo.serieses.listImportable(),
                "selectedCrawlSeriesId", parseLongOrDefault(request.queryParams("crawlSeries"), -1),
                "jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()),
                "csrfToken", Csrf.token(request));
    }

    String performImport(Request request, Response response) {
        String jobName = request.queryParams("heritrixJob");
        long crawlSeriesId = Long.parseLong(request.queryParams("crawlSeriesId"));
        long crawlId = bamboo.crawls.importHeritrixCrawl(jobName, crawlSeriesId);
        response.redirect(request.contextPath() + "/crawls/" + crawlId, 303);
        return "";
    }

    @Override
    public void close() {
        bamboo.close();
    }

    private Object serveStatic(Request request, Response response) {
        String path = request.uri().replace("/../", "/")
                .replaceAll("%2[Bb]", "+");
        if (path.endsWith(".css")) {
            response.type("text/css");
        } else if (path.endsWith(".js")) {
            response.type("application/javascript");
        } else if (path.endsWith(".woff")) {
            response.type("font/woff");
        } else if (path.endsWith(".woff2")) {
            response.type("font/woff2");
        } else if (path.endsWith(".svg")) {
            response.type("image/svg+xml");
        } else if (path.endsWith(".eot")) {
            response.type("application/vnd.ms-fontobject");
        } else if (path.endsWith(".png")) {
            response.type("image/png");
        } else if (path.endsWith(".jpg")) {
            response.type("image/jpeg");
        } else if (path.endsWith(".gif")) {
            response.type("image/gif");
        } else if (path.endsWith(".html")) {
            response.type("text/html");
        } else {
            response.type("application/octet-stream");
        }
        InputStream stream = getClass().getResourceAsStream("/META-INF/resources" + path);
        if (stream == null) {
            response.status(404);
            response.body("Not found");
            return null;
        }
        return stream;
    }

    public static class NotFound extends RuntimeException {
        public NotFound(String message) {
            super(message);
        }
    }
}
