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

public class Bamboo {
    final Config config = new Config();
    final DbPool dbPool = new DbPool(config);
    final Taskmaster taskmaster = new Taskmaster();

    Handler routes = routes(
            resources("/webjars", "META-INF/resources/webjars"),
            resources("/assets", "bamboo/assets"),
            GET("/", this::index),
            GET("/cdx/:id", this::showCdx, "id", "[0-9]+"),
            POST("/cdx/:id/calcstats", this::calcCdxStats, "id", "[0-9]+"),
            GET("/collection/:id", this::showCollection, "id", "[0-9]+"),
            GET("/import", this::showImportForm),
            GET("/tasks", this::showTasks),
            notFound("404. Alas, there is nothing here."));

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

    public static Handler startApp() throws IOException {
        Bamboo bamboo = new Bamboo();
        Configuration fremarkerConfig = FreeMarkerHandler.defaultConfiguration(Bamboo.class, "/bamboo/views");
        fremarkerConfig.addAutoInclude("layout.ftl");
        BeansWrapper beansWrapper = BeansWrapper.getDefaultInstance();
        beansWrapper.setExposeFields(true);
        fremarkerConfig.setObjectWrapper(beansWrapper);
        return new FreeMarkerHandler(fremarkerConfig, bamboo.routes);
    }

    public static void usage() {
        System.err.println("Usage: java " + Bamboo.class.getName() + " [-b bindaddr] [-p port] [-i]");
        System.err.println("");
        System.err.println("  -b bindaddr   Bind to a particular IP address");
        System.err.println("  -i            Inherit the server socket via STDIN (for use with systemd, inetd etc)");
        System.err.println("  -p port       Local port to listen on");
    }

    public static void main(String[] args) throws IOException {
        int port = 8080;
        String host = null;
        boolean inheritSocket = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-p")) {
                port = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-b")) {
                host = args[++i];
            } else if (args[i].equals("-i")) {
                inheritSocket = true;
            } else {
                usage();
                System.exit(1);
            }
        }
        Handler handler = startApp();
        if (inheritSocket) {
            Channel channel = System.inheritedChannel();
            if (channel != null && channel instanceof ServerSocketChannel) {
                new NanoServer(handler, ((ServerSocketChannel) channel).socket()).startAndJoin();
                System.exit(0);
            }
            System.err.println("When -i is given STDIN must be a ServerSocketChannel, but got " + channel);
            System.exit(1);
        }
        if (host != null) {
            new NanoServer(handler, host, port).startAndJoin();
        } else {
            new NanoServer(handler, port).startAndJoin();
        }
    }
}
