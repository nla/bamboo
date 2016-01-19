package bamboo.core;

import bamboo.util.Markdown;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import droute.Csrf;
import droute.Handler;
import droute.Request;
import droute.Response;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static droute.Response.*;
import static droute.Route.*;

public class SeriesController {
    final Bamboo bamboo;
    public final Handler routes = routes(
            GET("/series", this::index),
            GET("/series/new", this::newForm),
            POST("/series/new", this::createSeries),
            GET("/series/:id", this::show, "id", "[0-9]+"),
            GET("/series/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/series/:id/edit", this::update, "id", "[0-9]+"));

    public SeriesController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        Pager<Db.CrawlSeriesWithCount> pager = bamboo.serieses.paginate(Parsing.parseLongOrDefault(request.queryParam("page"), 1));
        return render("series/index.ftl",
                "seriesList", pager.items,
                "seriesPager", pager);
    }

    Response newForm(Request request) {
        return render("series/new.ftl", "csrfToken", Csrf.token(request));
    }

    Response createSeries(Request request) {
        long seriesId = bamboo.serieses.create(parseForm(request));
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
        Series series = bamboo.serieses.get(id);
        return render("series/show.ftl",
                "series", series,
                "descriptionHtml", Markdown.render(series.getDescription(), request.uri()),
                "crawls", bamboo.crawls.listWhereSeriesId(id),
                "collections", bamboo.collections.listWhereSeriesId(id));
    }

    Response edit(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Series series = bamboo.serieses.get(id);
        return render("series/edit.ftl",
                "series", series,
                "collections", bamboo.collections.listWhereSeriesId(id),
                "allCollections", bamboo.collections.listAll(),
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

        bamboo.serieses.update(seriesId, parseForm(request),
                collectionIds, collectionUrlFilters);
        return seeOther(request.contextUri().resolve("series/" + seriesId).toString());
    }
}
