package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Pager;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import static droute.Response.*;
import static droute.Route.*;

public class SeriesController {
    final Bamboo bamboo;
    public Handler routes = routes(
            GET("/series", this::index),
            GET("/series/new", this::newForm),
            POST("/series/new", this::createSeries),
            GET("/series/:id", this::show, "id", "[0-9]+"),
            GET("/series/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/series/:id/edit", this::update, "id", "[0-9]+"));

    public SeriesController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.CrawlSeries> pager = new Pager<>(request, "page", db.countCrawls(), db::paginateCrawlSeries);
            return render("series/index.ftl",
                    "seriesList", pager.items,
                    "seriesPager", pager);
        }
    }

    Response newForm(Request request) {
        return render("series/new.ftl", "csrfToken", Csrf.token(request));
    }

    Response createSeries(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            long seriesId = db.createCrawlSeries(request.formParam("name"), request.formParam("path"));
            return seeOther(request.contextUri().resolve("series/" + seriesId).toString());
        }
    }

    Response show(Request request) {
        long seriesId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.CrawlSeries series = db.findCrawlSeriesById(seriesId);
            if (series == null) {
                return notFound("No such crawl series: " + seriesId);
            }
            return render("series/show.ftl", "series", series, "crawls", db.findCrawlsByCrawlSeriesId(seriesId));
        }
    }

    Response edit(Request request) {
        long seriesId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.CrawlSeries series = db.findCrawlSeriesById(seriesId);
            if (series == null) {
                return notFound("No such crawl series: " + seriesId);
            }
            return render("series/edit.ftl", "series", series, "csrfToken", Csrf.token(request));
        }
    }

    Response update(Request request) {
        long seriesId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            if (db.updateCrawlSeries(seriesId, request.formParam("name"), request.formParam("path")) < 1) {
                return notFound("No such crawl series: " + seriesId);
            }
            return seeOther(request.contextUri().resolve("series/" + seriesId).toString());
        }
    }
}
