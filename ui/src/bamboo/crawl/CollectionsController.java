package bamboo.crawl;

import static droute.Response.response;
import static droute.Response.seeOther;
import static droute.Route.GET;
import static droute.Route.POST;
import static droute.Route.routes;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import bamboo.app.Bamboo;
import bamboo.task.WarcToIndex;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;
import droute.Streamable;

public class CollectionsController {

    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/collections", this::index),
            GET("/collections/new", this::newForm),
            POST("/collections/new", this::create),
            GET("/collections/:id", this::show, "id", "[0-9]+"),
            GET("/collections/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/collections/:id/edit", this::update, "id", "[0-9]+"),
            GET("/collections/:id/warcs/json", this::warcs, "id", "[0-9]+"),
            GET("/collections/:id/warcs/sync", this::sync, "id", "[0-9]+"));

    private static final Gson gson;
    static {
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String indent = System.getProperty("disableJsonIndent");
        if (indent != null && "true".equals(indent)) {
            gson = builder.create();
        } else {
            gson = builder.setPrettyPrinting().create();
        }
    }

    public CollectionsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response render(String view, Object... model) {
        return Response.render("/" + getClass().getName().replaceFirst("\\.[^.]*$","").replace('.', '/') + "/views/" + view, model);
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

    Response warcs(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        long start = Parsing.parseLongOrDefault(request.queryParam("start"), 0);
        long rows = Parsing.parseLongOrDefault(request.queryParam("rows"), 1000);
        List<Warc> warcs = bamboo.warcs.findByCollectionId(id, start, rows);
        return response(200, (Streamable) (OutputStream outStream) -> {
            JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
            writer.beginArray();
            for (Warc warc : warcs) {
                gson.toJson(new BambooWarcToIndex(warc), BambooWarcToIndex.class, writer);
            }
            writer.endArray();
            writer.flush();
        }).withHeader("Content-Type", "application/json");
    }

    Response sync(Request request) {
        long collectionId = Long.parseLong(request.urlParam("id"));
        String afterParam = request.queryParam("after");
        WarcResumptionToken after = afterParam == null ? WarcResumptionToken.MIN_VALUE : WarcResumptionToken.parse(afterParam);
        int limit = Parsing.parseIntOrDefault(request.queryParam("limit"), 100);
        List<WarcResumptionToken> results = bamboo.warcs.resumptionByCollectionIdAndStateId(collectionId, 2, after, limit);
        return response(200, (Streamable) (OutputStream outStream) -> {
            JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
            writer.beginArray();
            for (WarcResumptionToken token: results) {
                writer.beginObject();
                writer.name("id").value(token.id);
                writer.name("resumptionToken").value(token.toString());
                writer.endObject();
            }
            writer.endArray();
            writer.flush();
        }).withHeader("Content-Type", "application/json");

    }

    class BambooWarcToIndex extends WarcToIndex {
        public BambooWarcToIndex(Warc warc) {
            super(warc.getId(), warc.getRecords());
        }
    }
}
