package bamboo.core;

import bamboo.util.Markdown;
import bamboo.util.Pager;
import bamboo.util.Parsing;
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
        Pager<Collection> pager = bamboo.collections.paginate(Parsing.parseLongOrDefault(request.queryParam("page"), 1));
        return render("collections/index.ftl",
                "collections", pager.items,
                "collectionsPager", pager);
    }

    Response newForm(Request request) {
        return render("collections/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    Response create(Request request) {
        long collectionId = bamboo.collections.create(parseForm(request));
        return seeOther(request.contextUri().resolve("collections/" + collectionId).toString());
    }

    private Collection parseForm(Request request) {
        Collection collection = new Collection();
        collection.setName(request.formParam("name"));
        collection.setDescription(request.formParam("description"));
        collection.setCdxUrl(request.formParam("cdxUrl"));
        collection.setSolrUrl(request.formParam("solrUrl"));
        return collection;
    }

    Response show(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Collection collection = bamboo.collections.get(id);
        return render("collections/show.ftl",
                "collection", collection,
                "descriptionHtml", Markdown.render(collection.getDescription(), request.uri()));
    }

    Response edit(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Collection collection = bamboo.collections.get(id);
        return render("collections/edit.ftl",
                "collection", collection,
                "csrfToken", Csrf.token(request));
    }

    Response update(Request request) {
        long collectionId = Long.parseLong(request.urlParam("id"));
        bamboo.collections.update(collectionId, parseForm(request));
        return seeOther(request.contextUri().resolve("collections/" + collectionId).toString());
    }
}
