package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import static droute.Response.*;
import static droute.Route.*;

public class CrawlsController {
    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/crawls", this::index),
            GET("/crawls/:id", this::show, "id", "[0-9]+"),
            GET("/crawls/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/crawls/:id/edit", this::update, "id", "[0-9]+"),
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
                    "warcsToBeSolrIndexed", db.countWarcsToBeSolrIndexedInCrawl(crawlId),
                    "corruptWarcs", db.countCorruptWarcsInCrawl(crawlId),
                    "descriptionHtml", Markdown.render(crawl.description, request.uri())
                    );
        }
    }

    Response edit(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }
            return render("crawls/edit.ftl",
                    "crawl", crawl,
                    "csrfToken", Csrf.token(request)
            );
        }
    }

    Response update(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            String description = request.formParam("description");
            if (description != null && description.isEmpty()) {
                description = null;
            }
            int rows = db.updateCrawl(crawlId, request.formParam("name"), description);
            if (rows == 0) {
                return notFound("No such crawl: " + crawlId);
            }
            return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
        }
    }

    Response buildCdx(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        //bamboo.buildCdx(crawlId);
        return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
    }

}
