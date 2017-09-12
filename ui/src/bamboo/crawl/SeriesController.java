package bamboo.crawl;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import bamboo.app.Bamboo;
import bamboo.util.*;
import org.apache.commons.collections.ArrayStack;
import spark.Request;
import spark.Response;
import spark.Spark;

public class SeriesController {
    final Bamboo wa;
    public void routes() {
        Spark.get("/series", this::index);
        Spark.get("/series/new", this::newForm);
        Spark.post("/series/new", this::createSeries);
        Spark.get("/series/:id", this::show);
        Spark.get("/series/:id/edit", this::edit);
        Spark.post("/series/:id/edit", this::update);
    }

    public SeriesController(Bamboo wa) {
        this.wa = wa;
    }

    String render(Request request, String view, Object... model) {
        return Freemarker.render(request, "bamboo/crawl/views/" + view, model);
    }

    String index(Request request, Response response) {
        Pager<SeriesDAO.CrawlSeriesWithCount> pager = wa.serieses.paginate(Parsing.parseLongOrDefault(request.queryParams("page"), 1));
        return render(request, "series/index.ftl",
                "seriesList", pager.items,
                "seriesPager", pager);
    }

    String newForm(Request request, Response response) {
        return render(request, "series/new.ftl", "csrfToken", Csrf.token(request));
    }

    String createSeries(Request request, Response response) {
        long seriesId = wa.serieses.create(parseForm(request));
        response.redirect(request.contextPath() + "/series/" + seriesId, 303);
        return "";
    }

    private Series parseForm(Request request) {
        Series series = new Series();
        series.setName(request.queryParams("name"));
        series.setDescription(request.queryParams("description"));

        String path = request.queryParams("path");
        if (path != null && !path.isEmpty()) {
            series.setPath(Paths.get(path));
        }

        return series;
    }

    String show(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Series series = wa.serieses.get(id);
        long page = Parsing.parseLongOrDefault(request.queryParams("page"), 1);
        Pager<Crawl> crawlPager = wa.crawls.paginateWithSeriesId(page, id);
        return render(request, "series/show.ftl",
                "series", series,
                "descriptionHtml", Markdown.render(series.getDescription(), request.uri()),
                "crawlList", crawlPager.items,
                "crawlPager", crawlPager,
                "collections", wa.collections.listWhereSeriesId(id));
    }

    String edit(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Series series = wa.serieses.get(id);
        return render(request, "series/edit.ftl",
                "series", series,
                "collections", wa.collections.listWhereSeriesId(id),
                "allCollections", wa.collections.listAll(),
                "csrfToken", Csrf.token(request));
    }

    String update(Request request, Response response) {
        long seriesId = Long.parseLong(request.params(":id"));
        String[] collectionIdValues = request.queryParamsValues("collection.id");
        List<Long> collectionIds = new ArrayList<>();
        if (collectionIdValues != null) {
            for (String value: collectionIdValues) {
                collectionIds.add(Long.parseLong(value));
            }
        }
        String[] filterValues = request.queryParamsValues("collection.urlFilters");
        List<String> collectionUrlFilters = filterValues == null ? new ArrayList<>() : Arrays.asList(filterValues);

        if (collectionIds.size() != collectionUrlFilters.size()) {
            throw Spark.halt(400, "collection.id and collection.urlFilters mismatch");
        }

        wa.serieses.update(seriesId, parseForm(request),
                collectionIds, collectionUrlFilters);
        response.redirect(request.contextPath() + "/series/" + seriesId, 303);
        return "";
    }
}
