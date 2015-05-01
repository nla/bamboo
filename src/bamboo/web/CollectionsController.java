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

public class CollectionsController {
    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/collections", this::index),
            GET("/collections/new", this::newForm),
            POST("/collections/new", this::create),
            GET("/collections/:id", this::show, "id", "[0-9]+"),
            GET("/collections/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/collections/:id/edit", this::update, "id", "[0-9]+"));

    public CollectionsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.Collection> pager = new Pager<>(request, "page", db.countCollections(), db::paginateCollections);
            return render("collections/index.ftl",
                    "collections", pager.items,
                    "collectionsPager", pager);
        }
    }

    Response newForm(Request request) {
        return render("collections/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    Response create(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            long collectionId = db.createCollection(request.formParam("name"),
                    request.formParam("description"),
                    request.formParam("cdxUrl"),
                    request.formParam("solrUrl"));
            return seeOther(request.contextUri().resolve("collections/" + collectionId).toString());
        }
    }

    Response show(Request request) {
        long collectionId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Collection collection = db.findCollection(collectionId);
            if (collection == null) {
                return notFound("No such collection: " + collectionId);
            }
            return render("collections/show.ftl",
                    "collection", collection,
                    "descriptionHtml", Markdown.render(collection.description, request.uri()));
        }
    }

    Response edit(Request request) {
        long collectionId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Collection collection = db.findCollection(collectionId);
            if (collection == null) {
                return notFound("No such collection: " + collectionId);
            }
            return render("collections/edit.ftl",
                    "collection", collection,
                    "csrfToken", Csrf.token(request));
        }
    }

    Response update(Request request) {
        long collectionId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            int rows = db.updateCollection(collectionId, request.formParam("name"), request.formParam("description"),
                    request.formParam("cdxUrl"), request.formParam("solrUrl"));
            if (rows == 0) {
                return notFound("No such collection: " + collectionId);
            }
            return seeOther(request.contextUri().resolve("collections/" + collectionId).toString());
        }
    }
}
