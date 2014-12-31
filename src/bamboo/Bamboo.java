package bamboo;

import bamboo.task.CdxStatsJob;
import bamboo.task.Taskmaster;
import droute.*;
import droute.nanohttpd.NanoServer;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channel;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Paths;

import static droute.Response.redirect;
import static droute.Response.render;
import static droute.Response.response;
import static droute.Route.*;

public class Bamboo implements Handler, AutoCloseable {
    final Config config = new Config();
    final DbPool dbPool = new DbPool(config);
    final Taskmaster taskmaster = new Taskmaster();

    final Handler routes = routes(
            resources("/webjars", "META-INF/resources/webjars"),
            resources("/assets", "bamboo/assets"),
            GET("/", this::index),
            GET("/cdx/:id", this::showCdx, "id", "[0-9]+"),
            POST("/cdx/:id/calcstats", this::calcCdxStats, "id", "[0-9]+"),
            GET("/collection/:id", this::showCollection, "id", "[0-9]+"),
            GET("/import", this::showImportForm),
            GET("/tasks", this::showTasks),
            new SeriesController(this).routes,
            notFound("404. Alas, there is nothing here."));

    final Handler handler;

    public Bamboo() {
        dbPool.migrate();
        Configuration fremarkerConfig = FreeMarkerHandler.defaultConfiguration(Bamboo.class, "/bamboo/views");
        fremarkerConfig.addAutoInclude("layout.ftl");
        BeansWrapper beansWrapper = BeansWrapper.getDefaultInstance();
        beansWrapper.setExposeFields(true);
        fremarkerConfig.setObjectWrapper(beansWrapper);
        handler = new FreeMarkerHandler(fremarkerConfig, routes);
    }

    Response index(Request request) {
        try (Db db = dbPool.take()) {
            return render("index.ftl",
                    "collections", db.listCollections());
        }
    }

    Response showCdx(Request request) {
        try (Db db = dbPool.take()) {
            long id = Long.parseLong(request.param("id"));
            Db.Cdx cdx = db.findCdx(id);
            if (cdx == null) {
                return response(404, "No such CDX: " + id);
            }
            return render("cdx.ftl",
                    "cdx", cdx,
                    "crawls", db.findCrawlsByCdxId(id));
        }
    }

    Response calcCdxStats(Request request) {
        long id = Long.parseLong(request.param("id"));
        try (Db db = dbPool.take()) {
            Db.Cdx cdx = db.findCdx(id);
            if (cdx == null) {
                return response(404, "No such CDX: " + id);
            }
            taskmaster.launch(new CdxStatsJob(Paths.get(cdx.path)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to launch calcstats job for CDX " + id, e);
        }
        return redirect(request.contextUri().resolve("tasks").toString());
    }

    Response showCollection(Request request) {
        try (Db db = dbPool.take()) {
            long id = Long.parseLong(request.param("id"));
            Db.Collection collection = db.findCollection(id);
            if (collection == null) {
                return response(404, "No such collection: " + id);
            }
            return render("collection.ftl",
                    "collection", collection,
                    "cdxs", db.findCdxsByCollectionId(id));
        }
    }

    Response showImportForm(Request request) {
        try (Db db = dbPool.take()) {
            return render("import.ftl",
                    "allCrawlSeries", db.listCrawlSeries(),
                    "jobs", HeritrixJob.list(config.getHeritrixJobs()));
        }
    }

    Response performImport(Request request) {
        try (Db db = dbPool.take()) {
            String jobName = request.param("heritrixJob");
            HeritrixJob job = HeritrixJob.byName(config.getHeritrixJobs(), jobName);

            long crawlSeriesId = Long.parseLong(request.param("crawlSeriesId"));
            Db.CrawlSeries crawlSeries = db.findCrawlSeriesById(crawlSeriesId);
            if (crawlSeries == null) {
                return badRequest("No such crawl series: " + crawlSeriesId);
            }


            return response("todo");
        }
    }

    Response showTasks(Request request) {
        return render("tasks.ftl", "jobs", taskmaster.getJobs());
    }

    Response showThing(Request request) {
        return response("showing thing " + request.param("id"));
    }

    Response badRequest(String message) {
        return response(400, message);
    }

    @Override
    public void close() {
        dbPool.close();
    }

    @Override
    public Response handle(Request request) {
        return handler.handle(request);
    }
}
