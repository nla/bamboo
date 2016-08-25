package bamboo.crawl;

import static droute.Response.response;
import static droute.Response.seeOther;
import static droute.Route.GET;
import static droute.Route.POST;
import static droute.Route.routes;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import bamboo.app.Bamboo;
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

public class SeriesController {
    final Bamboo wa;
    public final Handler routes = routes(
            GET("/series", this::index),
            GET("/series/json", this::indexJson),
            GET("/series/new", this::newForm),
            POST("/series/new", this::createSeries),
            GET("/series/:id", this::show, "id", "[0-9]+"),
            GET("/series/:id/crawlList", this::crawlList, "id", "[0-9]+"),
            GET("/series/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/series/:id/edit", this::update, "id", "[0-9]+"));

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

    public SeriesController(Bamboo wa) {
        this.wa = wa;
    }

    Response render(String view, Object... model) {
        return Response.render("/" + getClass().getName().replaceFirst("\\.[^.]*$","").replace('.', '/') + "/views/" + view, model);
    }

    Response index(Request request) {
        Pager<SeriesDAO.CrawlSeriesWithCount> pager = wa.serieses.paginate(Parsing.parseLongOrDefault(request.queryParam("page"), 1));
        return render("series/index.ftl",
                "seriesList", pager.items,
                "seriesPager", pager);
    }

    Response indexJson(Request request) {
        long start = Parsing.parseLongOrDefault(request.queryParam("start"), 0);
        long rows = Parsing.parseLongOrDefault(request.queryParam("rows"), 1000);
        List<SeriesDAO.CrawlSeriesWithCount> series = wa.serieses.listFrom(start, rows);
        return response(200, (Streamable) (OutputStream outStream) -> {
            JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
            writer.beginArray();
            for (SeriesDAO.CrawlSeriesWithCount s : series) {
                gson.toJson(new SeriesSummary(s), SeriesSummary.class, writer);
            }
            writer.endArray();
            writer.flush();
        }).withHeader("Content-Type", "application/json");
    }

    Response newForm(Request request) {
        return render("series/new.ftl", "csrfToken", Csrf.token(request));
    }

    Response createSeries(Request request) {
        long seriesId = wa.serieses.create(parseForm(request));
        return seeOther(request.contextUri().resolve("series/" + seriesId).toString());
    }

    private Series parseForm(Request request) {
        Series series = new Series();
        series.setName(request.formParam("name"));
        series.setDescription(request.formParam("description"));

        String path = request.formParam("path");
        if (path != null && !path.isEmpty()) {
            series.setPath(Paths.get(path));
        }

        return series;
    }

    Response show(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Series series = wa.serieses.get(id);
        return render("series/show.ftl",
                "series", series,
                "descriptionHtml", Markdown.render(series.getDescription(), request.uri()),
                "crawls", wa.crawls.listBySeriesId(id),
                "collections", wa.collections.listWhereSeriesId(id));
    }

    Response crawlList(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        List<Crawl> crawls = wa.crawls.listBySeriesId(id);
        return response(200, (Streamable) (OutputStream outStream) -> {
            JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
            writer.beginArray();
            for (Crawl crawl : crawls) {
                gson.toJson(new CrawlSummary(crawl), CrawlSummary.class, writer);
            }
            writer.endArray();
            writer.flush();
        }).withHeader("Content-Type", "application/json");
    }

    Response edit(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Series series = wa.serieses.get(id);
        return render("series/edit.ftl",
                "series", series,
                "collections", wa.collections.listWhereSeriesId(id),
                "allCollections", wa.collections.listAll(),
                "csrfToken", Csrf.token(request));
    }

    Response update(Request request) {
        long seriesId = Long.parseLong(request.urlParam("id"));
        List<Long> collectionIds = request.formParams().getOrDefault("collection.id", Collections.emptyList())
                .stream().map(Long::parseLong).collect(Collectors.toList());
        List<String> collectionUrlFilters = request.formParams().getOrDefault("collection.urlFilters", Collections.emptyList());

        if (collectionIds.size() != collectionUrlFilters.size()) {
            return response(400, "collection.id and collection.urlFilters mismatch");
        }

        wa.serieses.update(seriesId, parseForm(request),
                collectionIds, collectionUrlFilters);
        return seeOther(request.contextUri().resolve("series/" + seriesId).toString());
    }

    // To send out as json for crawling Bamboo
    private class CrawlSummary {
      public long id;
      public String name;
      public long warcCount = 0;
      public CrawlSummary(Crawl crawl) {
        this.id = crawl.getId();
        this.name = crawl.getName();
        this.warcCount = crawl.getWarcFiles();
      }
    }

    private class SeriesSummary {
      public long id;
      public String name;
      public long crawlCount = 0;
      public SeriesSummary(SeriesDAO.CrawlSeriesWithCount series) {
        this.id = series.getId();
        this.name = series.getName();
        this.crawlCount = series.crawlCount;
      }
    }
}
