package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import droute.*;
import org.archive.url.SURT;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static droute.Response.*;
import static droute.Route.*;

public class SeedlistsController {

    final Bamboo bamboo;

    public final Handler routes = routes(
            GET("/seedlists", this::index),
            GET("/seedlists/new", this::newForm),
            POST("/seedlists/new", this::create),
            GET("/seedlists/:id", this::show, "id", "[0-9]+"),
            GET("/seedlists/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/seedlists/:id/edit", this::update, "id", "[0-9]+"),
            GET("/seedlists/:id/import", this::importForm, "id", "[0-9]+"),
            POST("/seedlists/:id/import", this::doImport, "id", "[0-9]+"),
            POST("/seedlists/:id/delete", this::delete, "id", "[0-9]+"),
            GET("/seedlists/:id/export/urls", this::exportUrls, "id", "[0-9]+"),
            GET("/seedlists/:id/export/surts", this::exportSurts, "id", "[0-9]+")
            );

    public SeedlistsController(Bamboo bamboo) {
            this.bamboo = bamboo;
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
        try (Db db = bamboo.dbPool.take()) {
            long seedlistId = db.createSeedlist(request.formParam("name"));
            return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
        }
    }

    Response show(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Seedlist seedlist = db.findSeedlist(seedlistId);
            if (seedlist == null) {
                return notFound("No such seedlist: " + seedlistId);
            }
            return render("seedlists/show.ftl",
                    "seedlist", seedlist,
                    "seeds", db.findSeedsBySeedListId(seedlistId));
        }
    }

    Response edit(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Seedlist seedlist = db.findSeedlist(seedlistId);
            if (seedlist == null) {
                return notFound("No such seedlist: " + seedlistId);
            }
            return render("seedlists/edit.ftl",
                    "csrfToken", Csrf.token(request),
                    "seedlist", seedlist);
        }
    }

    Response update(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            db.updateSeedlist(seedlistId, request.formParam("name"));
            return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
        }
    }

    Response importForm(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Seedlist seedlist = db.findSeedlist(seedlistId);
            if (seedlist == null) {
                return notFound("No such seedlist: " + seedlistId);
            }
            return render("seedlists/import.ftl",
                    "csrfToken", Csrf.token(request),
                    "seedlist", seedlist);
        }
    }

    static String toProtocolAgnosticSURT(String url) {
        return SURT.toSURT(url.replaceFirst("^[a-z]+://", "http://"));
    }

    Response doImport(Request request) {
        long seedlistId = Long.parseLong(request.urlParam("id"));
        List<String> urls = new ArrayList<>();
        List<String> surts = new ArrayList<>();
        for (String url : request.formParam("urls").split("\n")) {
            url = url.trim();
            if (url.isEmpty()) {
                continue;
            }
            String surt = toProtocolAgnosticSURT(url);
            urls.add(url);
            surts.add(surt);
        }
        try (Db db = bamboo.dbPool.take()){
            db.insertSeeds(seedlistId, urls, surts);
        }
        // TODO: handle duplicates
        return seeOther(request.contextUri().resolve("seedlists/" + seedlistId).toString());
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
