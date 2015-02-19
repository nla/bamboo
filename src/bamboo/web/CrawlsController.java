package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Pager;
import droute.Handler;
import droute.Request;
import droute.Response;

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
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.Crawl> pager = new Pager<>(request, "page", db.countCrawls(), db::paginateCrawls);
            return render("crawls/index.ftl",
                    "crawls", pager.items,
                    "crawlsPager", pager);
        }
    }

    Response show(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }
            return render("crawls/show.ftl",
                    "crawl", crawl,
                    "series", db.findCrawlSeriesById(crawl.crawlSeriesId),
                    "warcsToBeCdxIndexed", db.countWarcsToBeCdxIndexedInCrawl(crawlId),
                    "warcsToBeSolrIndexed", db.countWarcsToBeSolrIndexedInCrawl(crawlId)

                    );
        }
    }

    Response buildCdx(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        //bamboo.buildCdx(crawlId);
        return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
    }

}
