package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.io.HeritrixJob;
import droute.*;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;

import static bamboo.util.Parsing.parseLongOrDefault;
import static droute.Response.*;
import static droute.Route.*;

public class Webapp implements Handler, AutoCloseable {
    final Bamboo bamboo = new Bamboo();

    final Handler routes = routes(
            resources("/webjars", "META-INF/resources/webjars"),
            resources("/assets", "bamboo/assets"),
            GET("/", this::index),
            POST("/cdx/:id/calcstats", this::calcCdxStats, "id", "[0-9]+"),
            GET("/import", this::showImportForm),
            POST("/import", this::performImport),
            GET("/tasks", this::showTasks),
            new CollectionsController(bamboo).routes,
            new CrawlsController(bamboo).routes,
            new SeriesController(bamboo).routes,
            new TasksController(bamboo).routes,
            notFoundHandler("404. Alas, there is nothing here."));

    final Handler handler;

    public Webapp() {
        Configuration fremarkerConfig = FreeMarkerHandler.defaultConfiguration(Bamboo.class, "/bamboo/views");
        fremarkerConfig.addAutoInclude("layout.ftl");
        BeansWrapper beansWrapper = BeansWrapper.getDefaultInstance();
        beansWrapper.setExposeFields(true);
        fremarkerConfig.setObjectWrapper(beansWrapper);
        Handler handler = new FreeMarkerHandler(fremarkerConfig, routes);
        this.handler = Csrf.protect(handler);
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            return render("index.ftl",
                    "seriesList", db.listCrawlSeries(),
                    "collections", db.listCollections());
        }
    }

    Response calcCdxStats(Request request) {
/*        long id = Long.parseLong(request.param("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Cdx cdx = db.findCdx(id);
            if (cdx == null) {
                return response(404, "No such CDX: " + id);
            }
            bamboo.taskmaster.launch(new CdxStatsJob(Paths.get(cdx.path)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to launch calcstats job for CDX " + id, e);
        }*/
        return seeOther(request.contextUri().resolve("tasks").toString());
    }

    Response showCollection(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            long id = Long.parseLong(request.param("id"));
            Db.Collection collection = db.findCollection(id);
            if (collection == null) {
                return response(404, "No such collection: " + id);
            }
            return render("collection.ftl",
                    "collection", collection);
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
            bamboo.importHeritrixCrawl(jobName, crawlSeriesId);

            return seeOther(request.contextUri().resolve("crawls").toString());
        }
    }

    Response showTasks(Request request) {
        return render("tasks.ftl", "jobs", bamboo.taskmaster.getJobs());
    }

    Response showThing(Request request) {
        return response("showing thing " + request.param("id"));
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
}
