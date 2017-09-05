package bamboo.crawl;

import bamboo.app.Bamboo;
import bamboo.task.WarcToIndex;
import bamboo.util.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static bamboo.util.Freemarker.render;

public class CollectionsController {

    final Bamboo bamboo;

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

    public void routes() {
        Spark.get("/collections", this::index);
        Spark.get("/collections/new", this::newForm);
        Spark.post("/collections/new", this::create);
        Spark.get("/collections/:id", this::show);
        Spark.get("/collections/:id/edit", this::edit);
        Spark.post("/collections/:id/edit", this::update);
        Spark.get("/collections/:id/warcs/json", this::warcs);
        Spark.get("/collections/:id/warcs/sync", this::sync);
    }

    String index(Request request, Response response) {
        Pager<Collection> pager = bamboo.collections.paginate(Parsing.parseLongOrDefault(request.queryParams("page"), 1));
        return render(request, "bamboo/crawl/views/collections/index.ftl",
                "collections", pager.items,
                "collectionsPager", pager);
    }

    String newForm(Request request, Response response) {
        return render(request, "bamboo/crawl/views/collections/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    String create(Request request, Response response) {
        long collectionId = bamboo.collections.create(parseForm(request));
        response.redirect(request.contextPath() + "/collections/" + collectionId, 303);
        return "";
    }

    private Collection parseForm(Request request) {
        Collection collection = new Collection();
        collection.setName(request.queryParams("name"));
        collection.setDescription(request.queryParams("description"));
        collection.setCdxUrl(request.queryParams("cdxUrl"));
        collection.setSolrUrl(request.queryParams("solrUrl"));
        return collection;
    }

    String show(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Collection collection = bamboo.collections.get(id);
        return render(request, "bamboo/crawl/views/collections/show.ftl",
                "collection", collection,
                "descriptionHtml", Markdown.render(collection.getDescription(), request.uri()));
    }

    String edit(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Collection collection = bamboo.collections.get(id);
        return render(request, "bamboo/crawl/views/collections/edit.ftl",
                "collection", collection,
                "csrfToken", Csrf.token(request));
    }

    String update(Request request, Response response) {
        long collectionId = Long.parseLong(request.params(":id"));
        bamboo.collections.update(collectionId, parseForm(request));
        response.redirect(request.contextPath() + "/collections/" + collectionId, 303);
        return "";
    }

    String warcs(Request request, Response response) throws IOException {
        long id = Long.parseLong(request.params(":id"));
        long start = Parsing.parseLongOrDefault(request.queryParams("start"), 0);
        long rows = Parsing.parseLongOrDefault(request.queryParams("rows"), 1000);
        List<Warc> warcs = bamboo.warcs.findByCollectionId(id, start, rows);

        response.status(200);
        response.type("application/json");

        try (JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(response.raw().getOutputStream(), StandardCharsets.UTF_8))) {
            writer.beginArray();
            for (Warc warc : warcs) {
                gson.toJson(new BambooWarcToIndex(warc), BambooWarcToIndex.class, writer);
            }
            writer.endArray();
            writer.flush();
        }
        return "";
    }

    String sync(Request request, Response response) throws IOException {
        long collectionId = Long.parseLong(request.params(":id"));
        String afterParam = request.queryParams("after");
        WarcResumptionToken after = afterParam == null ? WarcResumptionToken.MIN_VALUE : WarcResumptionToken.parse(afterParam);
        int limit = Parsing.parseIntOrDefault(request.queryParams("limit"), 100);
        List<WarcResumptionToken> results = bamboo.warcs.resumptionByCollectionIdAndStateId(collectionId, 2, after, limit);

        response.status(200);
        response.type("application/json");

        try (JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(response.raw().getOutputStream(), StandardCharsets.UTF_8))) {
            writer.beginArray();
            for (WarcResumptionToken token: results) {
                writer.beginObject();
                writer.name("id").value(token.id);
                writer.name("resumptionToken").value(token.toString());
                writer.name("urlCount").value(token.urlCount);
                writer.endObject();
            }
            writer.endArray();
            writer.flush();
        }
        return "";
    }

    class BambooWarcToIndex extends WarcToIndex {
        public BambooWarcToIndex(Warc warc) {
            super(warc.getId(), warc.getRecords());
        }
    }
}
