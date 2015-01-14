package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Parsing;
import droute.Handler;
import droute.Request;
import droute.Response;

import static bamboo.util.Parsing.parseLongOrDefault;
import static droute.Response.*;
import static droute.Route.*;

public class CrawlsController {
    final Bamboo bamboo;
    public Handler routes = routes(
            GET("/crawls", this::index),
            GET("/crawls/:id", this::show, "id", "[0-9]+"),
            POST("/crawls/:id/buildcdx", this::buildCdx, "id", "[0-9]+"));

    public CrawlsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        long pageSize = 100;
        long page = parseLongOrDefault(request.queryParam("page"), 1);
        long offset = (page - 1) * pageSize;
        try (Db db = bamboo.dbPool.take()) {
            long lastPage = db.countCrawls() / pageSize + 1;
            return render("crawls/index.ftl",
                    "crawls", db.paginateCrawls(pageSize, offset),
                    "currentPage", page,
                    "lastPage", lastPage);
        }
    }

    Response show(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }
            return render("crawls/show.ftl", "crawl", crawl);
        }
    }

    Response buildCdx(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        bamboo.buildCdx(crawlId);
        return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
    }

}
