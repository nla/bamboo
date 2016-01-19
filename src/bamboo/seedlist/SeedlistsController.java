package bamboo.seedlist;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.core.PandasDb;
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

    Bamboo bamboo;

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
            this.bamboo = bamboo;
        }

    Response index(Request request) {
        return render("seedlists/index.ftl",
                "seedlists", bamboo.seedlists.listAll());
    }

    Response newForm(Request request) {
        return render("seedlists/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    Response saveComparison(Request request) {
        Comparison comparison = new Comparison(bamboo, request);
        return render("seedlists/new.ftl",
                "seeds", comparison.sublist(request.param("sublist")),
                "csrfToken", Csrf.token(request));
    }


    Response create(Request request) {
        Seedlist seedlist = parseForm(request);
        String seeds = request.formParam("seeds");
        long seedlistId = bamboo.seedlists.create(seedlist, parseSeeds(seeds));
        return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
    }

    private Seedlist parseForm(Request request) {
        Seedlist seedlist = new Seedlist();
        seedlist.setName(request.formParam("name"));
        seedlist.setDescription(request.formParam("description"));
        return seedlist;
    }

    Response show(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Seedlist seedlist = bamboo.seedlists.get(id);
        return render("seedlists/show.ftl",
                "seedlist", seedlist,
                "descriptionHtml", Markdown.render(seedlist.getDescription(), request.uri()),
                "seeds", bamboo.seedlists.listSeeds(id));
    }

    Response edit(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Seedlist seedlist = bamboo.seedlists.get(id);
        return render("seedlists/edit.ftl",
                "csrfToken", Csrf.token(request),
                "seedlist", seedlist,
                "seeds", bamboo.seedlists.listSeeds(id));
    }

    Response update(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        bamboo.seedlists.update(seedlistId, parseForm(request), parseSeeds(request.formParam("seeds")));
        return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
    }

    List<Seed> parseSeeds(String seedText) {
        List<Seed> seeds = new ArrayList<>();
        HashSet<String> seen = new HashSet<>();
        for (String url : seedText.split("\n")) {
            url = SURTTokenizer.addImpliedHttpIfNecessary(url.trim());
            if (!url.isEmpty() && seen.add(url)) {
                seeds.add(new Seed(url));
            }
        }
        return seeds;
    }


    Response delete(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        bamboo.seedlists.delete(seedlistId);
        return seeOther(request.contextUri().resolve("seedlists").toString());
    }

    Response export(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        return export(bamboo.seedlists.listSeeds(seedlistId), exportFormat(request.param("format")));
    }

    private Response exportComparison(Request request) {
        List<Seed> sublist = new Comparison(bamboo, request).sublist(request.param("sublist"));
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
        for (Seed seed : bamboo.seedlists.listSeeds(seedlistId)) {
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
        Seedlist seedlist = bamboo.seedlists.get(id);
        return render("seedlists/compare.ftl",
                "seedlist", seedlist,
                "seedlists", bamboo.seedlists.listAll());
    }

    Response showComparison(Request request) {
        Comparison comparison = new Comparison(bamboo, request);
        return render("seedlists/comparison.ftl",
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
        if (bamboo.pandasDbPool == null) {
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
        seedlist = bamboo.seedlists.get(id);
        seeds = seedsByCanonicalizedUrl(seedlistId);
        unmatched.addAll(seeds.values());

        /*
         * Now stream all the urls in the PANDAS database and look them up
         * one by one.  This whole procedure will need revisiting if PANDAS
         * gets a lot more titles but for now it's acceptable (< 2 seconds)
         * and doesn't require any caching or schema changes.
         */
        try (PandasDb pandasDb = bamboo.pandasDbPool.take();
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

        return render("seedlists/pandas.ftl",
                "seedlist", seedlist,
                "matches", matches,
                "unmatched", unmatchedSorted);
    }
}
