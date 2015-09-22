package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.io.HeritrixJob;
import droute.*;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;

import java.io.PrintWriter;
import java.io.StringWriter;

import static bamboo.util.Parsing.parseLongOrDefault;
import static droute.Response.*;
import static droute.Route.*;

public class Webapp implements Handler, AutoCloseable {
    final Bamboo bamboo = new Bamboo();

    final Handler routes = routes(
            resources("/webjars", "META-INF/resources/webjars"),
            resources("/assets", "bamboo/assets"),
            GET("/", this::index),
            GET("/import", this::showImportForm),
            POST("/import", this::performImport),
            new CollectionsController(bamboo).routes,
            new CrawlsController(bamboo).routes,
            new JobsController(bamboo).routes,
            new SeedlistsController(bamboo).routes,
            new SeriesController(bamboo).routes,
            new TasksController(bamboo).routes,
            new WarcsController(bamboo).routes,
            notFoundHandler("404. Alas, there is nothing here."));

    final Handler handler;

    public Webapp() {
        Configuration fremarkerConfig = FreeMarkerHandler.defaultConfiguration(Bamboo.class, "/bamboo/views");
        fremarkerConfig.addAutoInclude("layout.ftl");
        BeansWrapper beansWrapper = BeansWrapper.getDefaultInstance();
        beansWrapper.setExposeFields(true);
        fremarkerConfig.setObjectWrapper(beansWrapper);
        Handler handler = new FreeMarkerHandler(fremarkerConfig, routes);
        handler = Csrf.protect(handler);
        this.handler = errorHandler(handler);
        bamboo.startWorkerThreads();
    }

    /**
     * Dump a copy of the stack trace to the client on uncaught exceptions.
     */
    static Handler errorHandler(Handler handler) {
        return request -> {
            try {
                return handler.handle(request);
            } catch (NotFound e) {
                return notFound(e.getMessage());
            } catch (Throwable t) {
                StringWriter out = new StringWriter();
                t.printStackTrace();
                t.printStackTrace(new PrintWriter(out));
                return response(500, "Internal Server Error\n\n" + out.toString());
            }
        };
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            return render("index.ftl",
                    "seriesList", db.listCrawlSeries(),
                    "collections", db.listCollections());
        }
    }

    Response showImportForm(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            return render("import.ftl",
                    "allCrawlSeries", db.listCrawlSeries(),
                    "selectedCrawlSeriesId", parseLongOrDefault(request.queryParam("crawlSeries"), -1),
                    "jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()),
                    "csrfToken", Csrf.token(request));
        }
    }

    Response performImport(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            String jobName = request.param("heritrixJob");
            long crawlSeriesId = Long.parseLong(request.param("crawlSeriesId"));
            Db.CrawlSeries crawlSeries = db.findCrawlSeriesById(crawlSeriesId);
            if (crawlSeries == null) {
                return badRequest("No such crawl series: " + crawlSeriesId);
            }
            long crawlId = bamboo.importHeritrixCrawl(jobName, crawlSeriesId);
            return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
        }
    }

    Response badRequest(String message) {
        return response(400, message);
    }

    @Override
    public Response handle(Request request) {
        return handler.handle(request);
    }

    @Override
    public void close() throws Exception {
        bamboo.close();
    }

    static class NotFound extends RuntimeException {
        public NotFound(String message) {
            super(message);
        }
    }
}
