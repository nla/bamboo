package bamboo.crawl;

import static droute.Response.response;
import static droute.Response.seeOther;
import static droute.Route.GET;
import static droute.Route.POST;
import static droute.Route.routes;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import bamboo.app.Bamboo;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

public class SeriesController {
    final Bamboo wa;
    public final Handler routes = routes(
            GET("/series", this::index),
            GET("/series/new", this::newForm),
            POST("/series/new", this::createSeries),
            GET("/series/:id", this::show, "id", "[0-9]+"),
            GET("/series/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/series/:id/edit", this::update, "id", "[0-9]+"));

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
}
