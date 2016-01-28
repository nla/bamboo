package bamboo.seedlist;

import bamboo.core.PandasDb;
import bamboo.core.PandasDbPool;
import bamboo.app.Bamboo;
import bamboo.util.Markdown;
import droute.*;
import droute.Request;
import org.archive.url.SURT;
import org.archive.url.SURTTokenizer;
import org.archive.url.WaybackURLKeyMaker;
import org.skife.jdbi.v2.ResultIterator;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.UnaryOperator;

import static droute.Response.*;
import static droute.Route.GET;
import static droute.Route.POST;
import static java.util.function.UnaryOperator.identity;

public class SeedlistsController {

    private final Seedlists seedlists;
    private final PandasDbPool pandasDbPool;

    public final Handler routes = Route.routes(
            GET("/seedlists", this::index),
            GET("/seedlists/new", this::newForm),
            POST("/seedlists/create", this::create),
            GET("/seedlists/:id", this::show, "id", "[0-9]+"),
            GET("/seedlists/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/seedlists/:id/edit", this::update, "id", "[0-9]+"),
            POST("/seedlists/:id/delete", this::delete, "id", "[0-9]+"),
            GET("/seedlists/:id/export/:format", this::export, "id", "[0-9]+"),
            GET("/seedlists/:id/compare", this::compare, "id", "[0-9]+"),
            GET("/seedlists/:id/compare/pandas", this::compareWithPandas, "id", "[0-9]+"),
            GET("/seedlists/:id1/compare/:id2", this::showComparison, "id1", "[0-9]+", "id2", "[0-9]+"),
            GET("/seedlists/:id1/compare/:id2/:sublist/export/:format", this::exportComparison, "id1", "[0-9]+", "id2", "[0-9]+"),
            GET("/seedlists/:id1/compare/:id2/:sublist/saveas", this::saveComparison, "id1", "[0-9]+", "id2", "[0-9]+")
    );

    final static WaybackURLKeyMaker keyMaker = new WaybackURLKeyMaker();

    public SeedlistsController(Bamboo bamboo) {
        seedlists = bamboo.seedlists;
        pandasDbPool = bamboo.pandasDbPool;
    }

    Response render(String view, Object... model) {
        // FIXME: load templates relative to class?
        return Response.render("/" + getClass().getName().replaceFirst("\\.[^.]*$","").replace('.', '/') + "/" + view, model);
    }

    Response index(Request request) {
        return render("views/index.ftl",
                "seedlists", seedlists.listAll());
    }

    Response newForm(Request request) {
        return render("views/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    Response saveComparison(Request request) {
        Comparison comparison = new Comparison(seedlists, request);
        return render("views/new.ftl",
                "seeds", comparison.sublist(request.param("sublist")),
                "csrfToken", Csrf.token(request));
    }


    Response create(Request request) {
        long seedlistId = seedlists.create(new Form(request));
        return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
    }

    Response update(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        seedlists.update(seedlistId, new Form(request));
        return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
    }

    private static class Form implements Seedlists.Update {

        private final Request request;

        public Form(Request request) {
            this.request = request;
        }

        @Override
        public String getName() {
            return request.formParam("name");
        }

        @Override
        public String getDescription() {
            return request.formParam("description");
        }

        @Override
        public Collection<Seed> getSeeds() {
            Set<Seed> seeds = new HashSet<>();
            for (String url : request.formParam("seeds").split("\n")) {
                url = SURTTokenizer.addImpliedHttpIfNecessary(url.trim());
                seeds.add(new Seed(url));
            }
            return seeds;
        }
    }

    Response show(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Seedlist seedlist = seedlists.get(id);
        return render("views/show.ftl",
                "seedlist", seedlist,
                "descriptionHtml", Markdown.render(seedlist.getDescription(), request.uri()),
                "seeds", seedlists.listSeeds(id));
    }

    Response edit(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Seedlist seedlist = seedlists.get(id);
        return render("views/edit.ftl",
                "csrfToken", Csrf.token(request),
                "seedlist", seedlist,
                "seeds", seedlists.listSeeds(id));
    }


    Response delete(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        seedlists.delete(seedlistId);
        return seeOther(request.contextUri().resolve("seedlists").toString());
    }

    Response export(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        return export(seedlists.listSeeds(seedlistId), exportFormat(request.param("format")));
    }

    private Response exportComparison(Request request) {
        List<Seed> sublist = new Comparison(seedlists, request).sublist(request.param("sublist"));
        UnaryOperator<String> format = exportFormat(request.param("format"));
        return export(sublist, format);
    }

    private Response export(List<Seed> seeds, UnaryOperator<String> transformation) {
        return response((Streamable) out -> {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
            for (Seed seed : seeds) {
                writer.write(transformation.apply(seed.getUrl()));
                writer.write("\n");
            }
            writer.flush();
        });
    }

    UnaryOperator<String> exportFormat(String formatName) {
        switch (formatName) {
            case "urls": return UnaryOperator.identity();
            case "surts": return SURT::toSURT;
            default: throw new IllegalArgumentException("unsupported export format");
        }
    }

    Map<String, Seed> seedsByCanonicalizedUrl(long seedlistId) {
        Map<String, Seed> seeds = new HashMap<>();
        for (Seed seed : seedlists.listSeeds(seedlistId)) {
            try {
                seeds.put(keyMaker.makeKey(seed.getUrl()), seed);
            } catch (URISyntaxException e) {
                // skip it
            }
        }
        return seeds;
    }

    Response compare(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Seedlist seedlist = seedlists.get(id);
        return render("views/compare.ftl",
                "seedlist", seedlist,
                "seedlists", seedlists.listAll());
    }

    Response showComparison(Request request) {
        Comparison comparison = new Comparison(seedlists, request);
        return render("views/comparison.ftl",
                "seedlist1", comparison.seedlist1,
                "seedlist2", comparison.seedlist2,
                "onlyIn1", comparison.onlyIn1,
                "onlyIn2", comparison.onlyIn2,
                "common", comparison.common);
    }

    public static class PandasMatch {
        public final Seed seed;
        public final PandasDb.Title title;

        public PandasMatch(Seed seed, PandasDb.Title title) {
            this.seed = seed;
            this.title = title;
        }
    }

    Response compareWithPandas(Request request) {
        if (pandasDbPool == null) {
            return notFound("PANDAS integration is not configured");
        }

        /*
         * We need to match the normalized version of all seed urls against the
         * PANDAS database. Since we don't store normalized URLs in either
         * database we have to fetch both lists and compare them.
         */

        long seedlistId = Long.parseLong(request.urlParam("id"));
        Seedlist seedlist;
        Map<String, Seed> seeds;
        Set<Seed> unmatched = new HashSet<>();
        List<PandasMatch> matches = new ArrayList<>();

        /*
         * So first let's put all the seeds into a hashmap keyed by normalized url.
         */

        long id = Long.parseLong(request.urlParam("id"));
        seedlist = seedlists.get(id);
        seeds = seedsByCanonicalizedUrl(seedlistId);
        unmatched.addAll(seeds.values());

        /*
         * Now stream all the urls in the PANDAS database and look them up
         * one by one.  This whole procedure will need revisiting if PANDAS
         * gets a lot more titles but for now it's acceptable (< 2 seconds)
         * and doesn't require any caching or schema changes.
         */
        try (PandasDb pandasDb = pandasDbPool.take();
             ResultIterator<PandasDb.Title> it = pandasDb.iterateTitles()) {
            while (it.hasNext()) {
                PandasDb.Title title = it.next();
                if (title.gatherUrl == null) {
                    continue;
                }
                String url = title.gatherUrl.trim();
                if (url.isEmpty()) {
                    continue;
                }
                String key = null;
                try {
                    key = keyMaker.makeKey(title.gatherUrl);
                } catch (URISyntaxException e) {
                    continue;
                }
                Seed seed = seeds.get(key);
                if (seed != null) {
                    matches.add(new PandasMatch(seed, title));
                    unmatched.remove(seed);
                }
            }
        }

        matches.sort((a, b) -> a.seed.getSurt().compareTo(b.seed.getSurt()));
        List<Seed> unmatchedSorted = new ArrayList<>(unmatched);
        unmatchedSorted.sort((a, b) -> a.getSurt().compareTo(b.getSurt()));

        return render("views/pandas.ftl",
                "seedlist", seedlist,
                "matches", matches,
                "unmatched", unmatchedSorted);
    }
}
