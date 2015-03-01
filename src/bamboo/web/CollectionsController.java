package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import static droute.Response.notFound;
import static droute.Response.render;
import static droute.Response.seeOther;
import static droute.Route.GET;
import static droute.Route.POST;
import static droute.Route.routes;

public class CollectionsController {
    final Bamboo bamboo;
    public Handler routes = routes(
            //GET("/collections", this::index),
            GET("/collections/:id", this::show, "id", "[0-9]+"),
            GET("/collections/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/collections/:id/edit", this::update, "id", "[0-9]+"));

    public CollectionsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response show(Request request) {
        long collectionId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Collection collection = db.findCollection(collectionId);
            if (collection == null) {
                return notFound("No such collection: " + collectionId);
            }
            return render("collections/show.ftl",
                    "collection", collection);
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
            int rows = db.updateCollection(collectionId, request.formParam("name"), request.formParam("cdxUrl"), request.formParam("solrUrl"));
            if (rows == 0) {
                return notFound("No such collection: " + collectionId);
            }
            return seeOther(request.contextUri().resolve("collections/" + collectionId).toString());
        }
    }
}
