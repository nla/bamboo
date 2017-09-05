package bamboo.seedlist;

import bamboo.app.Bamboo;
import bamboo.pandas.Pandas;
import bamboo.pandas.PandasComparison;
import bamboo.util.Csrf;
import bamboo.util.Freemarker;
import bamboo.util.Markdown;
import org.archive.url.SURT;
import org.archive.url.SURTTokenizer;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

public class SeedlistsController {

    private final Seedlists seedlists;
    private final Pandas pandas;

    public void routes() {
        Spark.get("/seedlists", this::index);
        Spark.get("/seedlists/new", this::newForm);
        Spark.post("/seedlists/create", this::create);
        Spark.get("/seedlists/:id", this::show);
        Spark.get("/seedlists/:id/edit", this::edit);
        Spark.post("/seedlists/:id/edit", this::update);
        Spark.post("/seedlists/:id/delete", this::delete);
        Spark.get("/seedlists/:id/export/:format", this::export);
        Spark.get("/seedlists/:id/compare", this::compare);
        Spark.get("/seedlists/:id/compare/pandas", this::compareWithPandas);
        Spark.get("/seedlists/:id1/compare/:id2", this::showComparison);
        Spark.get("/seedlists/:id1/compare/:id2/:sublist/export/:format", this::exportComparison);
        Spark.get("/seedlists/:id1/compare/:id2/:sublist/saveas", this::saveComparison);
    }

    public SeedlistsController(Bamboo bamboo) {
        seedlists = bamboo.seedlists;
        pandas = bamboo.pandas;
    }

    String render(Request request, String view, Object... model) {
        return Freemarker.render(request, "bamboo/seedlist/" + view, model);
    }

    String index(Request request, Response response) {
        return render(request, "views/index.ftl",
                "seedlists", seedlists.listAll());
    }

    String newForm(Request request, Response response) {
        return render(request, "views/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    String saveComparison(Request request, Response response) {
        Comparison comparison = new Comparison(seedlists,
                Long.parseLong(request.params(":id1")),
                Long.parseLong(request.params(":id2")));
        return render(request, "views/new.ftl",
                "seeds", comparison.sublist(request.params(":sublist")),
                "csrfToken", Csrf.token(request));
    }

    String create(Request request, Response response) {
        long seedlistId = seedlists.create(new Form(request));
        response.redirect(request.contextPath() + "/seedlists/" + seedlistId, 303);
        return "";
    }

    String update(Request request, Response response) {
        long seedlistId = Long.parseLong(request.params(":id"));
        seedlists.update(seedlistId, new Form(request));
        response.redirect(request.contextPath() + "/seedlists/" + seedlistId, 303);
        return "";
    }

    private static class Form implements Seedlists.Update {

        private final Request request;

        public Form(Request request) {
            this.request = request;
        }

        @Override
        public String getName() {
            return request.queryParams("name");
        }

        @Override
        public String getDescription() {
            return request.queryParams("description");
        }

        @Override
        public Collection<Seed> getSeeds() {
            Set<Seed> seeds = new HashSet<>();
            for (String url : request.queryParams("seeds").split("\n")) {
                url = SURTTokenizer.addImpliedHttpIfNecessary(url.trim());
                seeds.add(new Seed(url));
            }
            return seeds;
        }
    }

    String show(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Seedlist seedlist = seedlists.get(id);
        return render(request, "views/show.ftl",
                "seedlist", seedlist,
                "descriptionHtml", Markdown.render(seedlist.getDescription(), request.uri()),
                "seeds", seedlists.listSeeds(id));
    }

    String edit(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Seedlist seedlist = seedlists.get(id);
        return render(request, "views/edit.ftl",
                "csrfToken", Csrf.token(request),
                "seedlist", seedlist,
                "seeds", seedlists.listSeeds(id));
    }


    String delete(Request request, Response response) {
        long seedlistId = Long.parseLong(request.params(":id"));
        seedlists.delete(seedlistId);
        response.redirect(request.contextPath() + "/seedlists", 303);
        return "";
    }

    String export(Request request, Response response) throws IOException {
        long seedlistId = Long.parseLong(request.params(":id"));
        return export(seedlists.listSeeds(seedlistId), exportFormat(request.params(":format")), response);
    }

    private String exportComparison(Request request, Response response) throws IOException {
        List<Seed> sublist = new Comparison(seedlists,
                Long.parseLong(request.params(":id1")),
                Long.parseLong(request.params(":id2"))).sublist(request.params(":sublist"));
        UnaryOperator<String> format = exportFormat(request.params(":format"));
        return export(sublist, format, response);
    }

    private String export(List<Seed> seeds, UnaryOperator<String> transformation, Response response) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.raw().getOutputStream(), StandardCharsets.UTF_8))) {
            for (Seed seed : seeds) {
                writer.write(transformation.apply(seed.getUrl()));
                writer.write("\n");
            }
            writer.flush();
            return "";
        }
    }

    UnaryOperator<String> exportFormat(String formatName) {
        switch (formatName) {
            case "urls":
                return UnaryOperator.identity();
            case "surts":
                return SURT::toSURT;
            default:
                throw new IllegalArgumentException("unsupported export format");
        }
    }

    String compare(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Seedlist seedlist = seedlists.get(id);
        return render(request, "views/compare.ftl",
                "seedlist", seedlist,
                "seedlists", seedlists.listAll());
    }

    String showComparison(Request request, Response response) {
        Comparison comparison = new Comparison(seedlists,
                Long.parseLong(request.params(":id1")),
                Long.parseLong(request.params(":id2")));
        return render(request, "views/comparison.ftl",
                "seedlist1", comparison.seedlist1,
                "seedlist2", comparison.seedlist2,
                "onlyIn1", comparison.onlyIn1,
                "onlyIn2", comparison.onlyIn2,
                "common", comparison.common);
    }

    String compareWithPandas(Request request, Response response) {
        if (pandas == null) {
            throw Spark.halt(404, "PANDAS integration not configured");
        }

        long seedlistId = Long.parseLong(request.params(":id"));
        Seedlist seedlist = seedlists.get(seedlistId);
        PandasComparison comparison = pandas.compareSeedlist(seedlistId);

        return render(request, "views/pandas.ftl",
                "seedlist", seedlist,
                "matches", comparison.matches,
                "unmatched", comparison.unmatched);
    }
}
