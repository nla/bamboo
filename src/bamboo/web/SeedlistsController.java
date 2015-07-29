package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.core.PandasDb;
import bamboo.util.Markdown;
import droute.*;
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

class SeedlistsController {

    final Bamboo bamboo;

    final Handler routes = Route.routes(
            GET("/seedlists", this::index),
            GET("/seedlists/new", this::newForm),
            POST("/seedlists/new", this::create),
            GET("/seedlists/:id", this::show, "id", "[0-9]+"),
            GET("/seedlists/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/seedlists/:id/edit", this::update, "id", "[0-9]+"),
            POST("/seedlists/:id/delete", this::delete, "id", "[0-9]+"),
            GET("/seedlists/:id/export/urls", this::exportUrls, "id", "[0-9]+"),
            GET("/seedlists/:id/export/surts", this::exportSurts, "id", "[0-9]+"),
            GET("/seedlists/:id/compare/pandas", this::compareWithPandas, "id", "[0-9]+")
    );

    final static WaybackURLKeyMaker keyMaker = new WaybackURLKeyMaker();

    SeedlistsController(Bamboo bamboo) {
            this.bamboo = bamboo;
        }

    static Db.Seedlist findSeedlist(Db db, Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Db.Seedlist seedlist = db.findSeedlist(id);
        if (seedlist == null) {
            throw new Webapp.NotFound("No such seedlist: " + id);
        }
        return seedlist;
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            return render("seedlists/index.ftl",
                    "seedlists", db.listSeedlists());
        }
    }

    Response newForm(Request request) {
        return render("seedlists/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    Response create(Request request) {
        String name = request.formParam("name");
        String description = request.formParam("description");
        String seeds = request.formParam("seeds");
        try (Db db = bamboo.dbPool.take()) {
            long seedlistId = db.createSeedlist(name, description);
            if (seeds != null) {
                ParsedSeeds parsed = new ParsedSeeds(seeds);
                db.insertSeeds(seedlistId, parsed.urls, parsed.surts);
            }
            return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
        }
    }

    Response show(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Db.Seedlist seedlist = findSeedlist(db, request);
            return render("seedlists/show.ftl",
                    "seedlist", seedlist,
                    "descriptionHtml", Markdown.render(seedlist.description, request.uri()),
                    "seeds", db.findSeedsBySeedListId(seedlist.id));
        }
    }

    Response edit(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Db.Seedlist seedlist = findSeedlist(db, request);
            return render("seedlists/edit.ftl",
                    "csrfToken", Csrf.token(request),
                    "seedlist", seedlist,
                    "seeds", db.findSeedsBySeedListId(seedlist.id));
        }
    }

    Response update(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        String name = request.formParam("name");
        String description = request.formParam("description");
        String seeds = request.formParam("seeds");
        if (seeds != null) {
            ParsedSeeds parsed = new ParsedSeeds(seeds);
            try (Db db = bamboo.dbPool.take()) {
                db.updateSeedlist(seedlistId, name, description, parsed.urls, parsed.surts);
            }
        } else {
            try (Db db = bamboo.dbPool.take()) {
                db.updateSeedlist(seedlistId, name, description);
            }
        }
        return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
    }

    private static class ParsedSeeds {
        final List<String> urls = new ArrayList<>();
        final List<String> surts = new ArrayList<>();

        ParsedSeeds(String seedText) {
            HashSet<String> seen = new HashSet<>();
            for (String url : seedText.split("\n")) {
                url = canonicalize(url);
                if (!url.isEmpty() && seen.add(url)) {
                    urls.add(url);
                    surts.add(toProtocolAgnosticSURT(url));
                }
            }
        }

        static String canonicalize(String url) {
            return SURTTokenizer.addImpliedHttpIfNecessary(url.trim());
        }
    }

    static String toProtocolAgnosticSURT(String url) {
        return SURT.toSURT(url.replaceFirst("^[a-z]+://", "http://"));
    }

    Response delete(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            db.deleteSeedlist(seedlistId);
        }
        return seeOther(request.contextUri().resolve("seedlists").toString());
    }

    Response export(Request request, UnaryOperator<String> transformation) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        List<Db.Seed> seeds;
        try (Db db = bamboo.dbPool.take()) {
            seeds = db.findSeedsBySeedListId(seedlistId);
        }
        return response((Streamable) out -> {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
            for (Db.Seed seed : seeds) {
                writer.write(transformation.apply(seed.url));
                writer.write("\n");
            }
            writer.flush();
        });
    }

    Response exportUrls(Request request) {
        return export(request, UnaryOperator.<String>identity());
    }

    Response exportSurts(Request request) {
        return export(request, SURT::toSURT);
    }

    Map<String, Db.Seed> seedsByCanonicalizedUrl(Db db, long seedlistId) {
        Map<String, Db.Seed> seeds = new HashMap<>();
        for (Db.Seed seed : db.findSeedsBySeedListId(seedlistId)) {
            try {
                seeds.put(keyMaker.makeKey(seed.url), seed);
            } catch (URISyntaxException e) {
                // skip it
            }
        }
        return seeds;
    }

    public static class PandasMatch {
        public final Db.Seed seed;
        public final PandasDb.Title title;

        public PandasMatch(Db.Seed seed, PandasDb.Title title) {
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
        Db.Seedlist seedlist;
        Map<String, Db.Seed> seeds;
        Set<Db.Seed> unmatched = new HashSet<>();
        List<PandasMatch> matches = new ArrayList<>();

        /*
         * So first let's put all the seeds into a hashmap keyed by normalized url.
         */

        try (Db db = bamboo.dbPool.take()) {
            seedlist = findSeedlist(db, request);
            seeds = seedsByCanonicalizedUrl(db, seedlistId);
        }
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
                Db.Seed seed = seeds.get(key);
                if (seed != null) {
                    matches.add(new PandasMatch(seed, title));
                    unmatched.remove(seed);
                }
            }
        }

        matches.sort((a, b) -> a.seed.surt.compareTo(b.seed.surt));
        List<Db.Seed> unmatchedSorted = new ArrayList<>(unmatched);
        unmatchedSorted.sort((a, b) -> a.surt.compareTo(b.surt));

        return render("seedlists/pandas.ftl",
                "seedlist", seedlist,
                "matches", matches,
                "unmatched", unmatchedSorted);
    }
}
