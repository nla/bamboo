package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static droute.Response.*;
import static droute.Route.*;

public class SeriesController {
    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/series", this::index),
            GET("/series/new", this::newForm),
            POST("/series/new", this::createSeries),
            GET("/series/:id", this::show, "id", "[0-9]+"),
            GET("/series/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/series/:id/edit", this::update, "id", "[0-9]+"));

    public SeriesController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    static Db.CrawlSeries findCrawlSeries(Db db, Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Db.CrawlSeries series = db.findCrawlSeriesById(id);
        if (series == null) {
            throw new Webapp.NotFound("No such crawl series: " + id);
        }
        return series;
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.CrawlSeriesWithCount> pager = new Pager<>(request, "page", db.countCrawlSeries(), db::paginateCrawlSeries);
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
        try (Db db = bamboo.dbPool.take()) {
            Db.CrawlSeries series = findCrawlSeries(db, request);
            return render("series/show.ftl",
                    "series", series,
                    "descriptionHtml", Markdown.render(series.description, request.uri()),
                    "crawls", db.findCrawlsByCrawlSeriesId(series.id),
                    "collections", db.listCollectionsForCrawlSeries(series.id));
        }
    }

    Response edit(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Db.CrawlSeries series = findCrawlSeries(db, request);
            return render("series/edit.ftl",
                    "series", series,
                    "collections", db.listCollectionsForCrawlSeries(series.id),
                    "allCollections", db.listCollections(),
                    "csrfToken", Csrf.token(request));
        }
    }

    Response update(Request request) {
        long seriesId = Long.parseLong(request.urlParam("id"));
        List<Long> collectionIds = request.formParams().getOrDefault("collection.id", Collections.emptyList())
                .stream().map(Long::parseLong).collect(Collectors.toList());
        List<String> collectionUrlFilters = request.formParams().getOrDefault("collection.urlFilters", Collections.emptyList());

        if (collectionIds.size() != collectionUrlFilters.size()) {
            return response(400, "collection.id and collection.urlFilters mismatch");
        }

        try (Db db = bamboo.dbPool.take()) {
            int rows = db.updateCrawlSeries(seriesId, request.formParam("name"), request.formParam("path"),
                    request.formParam("description"),
                    collectionIds, collectionUrlFilters);
            if (rows == 0) {
                return notFound("No such crawl series: " + seriesId);
            }
        }
        return seeOther(request.contextUri().resolve("series/" + seriesId).toString());
    }
}
