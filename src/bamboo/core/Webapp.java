package bamboo.core;

import bamboo.crawl.CollectionsController;
import bamboo.crawl.CrawlsController;
import bamboo.crawl.SeriesController;
import bamboo.crawl.WarcsController;
import bamboo.task.HeritrixJob;
import bamboo.seedlist.SeedlistsController;
import bamboo.task.JobsController;
import bamboo.task.TasksController;
import droute.*;
import droute.nanohttpd.NanoServer;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.channels.Channel;
import java.nio.channels.ServerSocketChannel;

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
        Configuration fremarkerConfig = FreeMarkerHandler.defaultConfiguration(Bamboo.class, "/");
        fremarkerConfig.addAutoInclude("/bamboo/views/layout.ftl");
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
            } catch (NotFoundException e) {
                return notFound("Not found: " + e.getMessage());
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

    private static void usage() {
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
            switch (args[i]) {
                case "-p":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                    host = args[++i];
                    break;
                case "-i":
                    inheritSocket = true;
                    break;
                default:
                    usage();
                    System.exit(1);
            }
        }
        Handler handler = new ShotgunHandler("bamboo.web.Webapp");
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

    Response index(Request request) {
        return render("index.ftl",
                "seriesList", bamboo.serieses.listAll(),
                "collections", bamboo.collections.listAll());
    }

    Response showImportForm(Request request) {
        return render("import.ftl",
                "allCrawlSeries", bamboo.serieses.listImportable(),
                "selectedCrawlSeriesId", parseLongOrDefault(request.queryParam("crawlSeries"), -1),
                "jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()),
                "csrfToken", Csrf.token(request));
    }

    Response performImport(Request request) {
        String jobName = request.param("heritrixJob");
        long crawlSeriesId = Long.parseLong(request.param("crawlSeriesId"));
        long crawlId = bamboo.importHeritrixCrawl(jobName, crawlSeriesId);
        return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
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

    public static class NotFound extends RuntimeException {
        public NotFound(String message) {
            super(message);
        }
    }
}
