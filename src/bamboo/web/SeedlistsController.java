package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Markdown;
import droute.*;
import org.archive.url.SURT;
import org.archive.url.SURTTokenizer;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.UnaryOperator;

import static droute.Response.*;
import static droute.Route.*;

class SeedlistsController {

    final Bamboo bamboo;

    final Handler routes = routes(
            GET("/seedlists", this::index),
            GET("/seedlists/new", this::newForm),
            POST("/seedlists/new", this::create),
            GET("/seedlists/:id", this::show, "id", "[0-9]+"),
            GET("/seedlists/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/seedlists/:id/edit", this::update, "id", "[0-9]+"),
            POST("/seedlists/:id/delete", this::delete, "id", "[0-9]+"),
            GET("/seedlists/:id/export/urls", this::exportUrls, "id", "[0-9]+"),
            GET("/seedlists/:id/export/surts", this::exportSurts, "id", "[0-9]+")
            );

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

}