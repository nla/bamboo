package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
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
            POST("/series/new", this::createSeries)
            );

    public SeriesController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            return render("series/index.ftl", "seriesList", db.listCrawlSeries());
        }
    }

    Response newForm(Request request) {
        return render("series/new.ftl");
    }

    Response createSeries(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            long seriesId = db.createCrawlSeries(request.formParam("name"), request.formParam("path"));
            return seeOther("/series");
        }
    }

}
