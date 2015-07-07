package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import droute.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.zip.*;

import static droute.Response.*;
import static droute.Route.GET;
import static droute.Route.POST;

public class CrawlsController {
    final Bamboo bamboo;
    public final Handler routes = Route.routes(
            GET("/crawls", this::index),
            GET("/crawls/:id", this::show, "id", "[0-9]+"),
            GET("/crawls/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/crawls/:id/edit", this::update, "id", "[0-9]+"),
            GET("/crawls/:id/warcs", this::listWarcs, "id", "[0-9]+"),
            GET("/crawls/:id/warcs/download", this::downloadWarcs, "id", "[0-9]+"),
            GET("/crawls/:id/warcs/corrupt", this::listCorruptWarcs, "id", "[0-9]+"),
            GET("/crawls/:id/reports", this::listReports, "id", "[0-9]+")
            );

    public CrawlsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    Response index(Request request) {
        try (Db db = bamboo.dbPool.take()) {
            Pager<Db.CrawlWithSeriesName> pager = new Pager<>(request, "page", db.countCrawls(), db::paginateCrawlsWithSeriesName);
            return render("crawls/index.ftl",
                    "crawls", pager.items,
                    "crawlsPager", pager);
        }
    }

    Response show(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }

            return render("crawls/show.ftl",
                    "crawl", crawl,
                    "series", db.findCrawlSeriesById(crawl.crawlSeriesId),
                    "warcsToBeCdxIndexed", db.countWarcsToBeCdxIndexedInCrawl(crawlId),
                    "warcsToBeSolrIndexed", db.countWarcsToBeSolrIndexedInCrawl(crawlId),
                    "corruptWarcs", db.countCorruptWarcsInCrawl(crawlId),
                    "descriptionHtml", Markdown.render(crawl.description, request.uri())
                    );
        }
    }

    Response edit(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }
            return render("crawls/edit.ftl",
                    "crawl", crawl,
                    "csrfToken", Csrf.token(request)
            );
        }
    }

    Response update(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            String description = request.formParam("description");
            if (description != null && description.isEmpty()) {
                description = null;
            }
            int rows = db.updateCrawl(crawlId, request.formParam("name"), description);
            if (rows == 0) {
                return notFound("No such crawl: " + crawlId);
            }
            return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
        }
    }

    Response listWarcs(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }
            Pager<Db.Warc> pager = new Pager<>(request, "page", crawl.warcFiles,
                    (limit, offset) -> db.paginateWarcsInCrawl(crawlId, limit, offset));
            return render("crawls/warcs.ftl",
                    "crawl", crawl,
                    "warcs", pager.items,
                    "warcsPager", pager);
        }
    }

    Response listCorruptWarcs(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }
            Pager<Db.Warc> pager = new Pager<>(request, "page", db.countCorruptWarcsInCrawl(crawlId),
                    (limit, offset) -> db.paginateCorruptWarcsInCrawl(crawlId, limit, offset));
            return render("crawls/warcs.ftl",
                    "titlePrefix", "Corrupt",
                    "crawl", crawl,
                    "warcs", pager.items,
                    "warcsPager", pager);
        }
    }

    Response downloadWarcs(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        List<Db.Warc> warcs;
        try (Db db = bamboo.dbPool.take()) {
            warcs = db.findWarcsByCrawlId(crawlId);
        }
        if (warcs.isEmpty()) {
            return response(404, "No warcs found");
        }
        return response((Streamable) out -> {
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                for (Db.Warc warc : warcs) {
                    writeZipEntry(zip, "crawl-" + crawlId + "/" + warc.filename, warc.path);
                }
            }
        }).withHeader("Content-Type", "application/zip")
          .withHeader("Content-Disposition", "attachment; filename=crawl-" + crawlId + ".zip");
    }

    private void writeZipEntry(ZipOutputStream zip, String entryName, Path sourceFile) throws IOException {
        long size = Files.size(sourceFile);

        ZipEntry entry = new ZipEntry(entryName);
        entry.setLastModifiedTime(Files.getLastModifiedTime(sourceFile));
        entry.setSize(size);
        entry.setCompressedSize(size);
        entry.setCrc(crc32(sourceFile));

        zip.setMethod(ZipOutputStream.STORED);
        zip.putNextEntry(entry);
        Files.copy(sourceFile, zip);
        zip.closeEntry();
    }

    private long crc32(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file)) {
            CRC32 crc32 = new CRC32();
            ByteBuffer buffer = ByteBuffer.allocate(16384);
            while (channel.read(buffer) >= 0) {
                buffer.flip();
                crc32.update(buffer);
                buffer.clear();
            }
            return crc32.getValue();
        }
    }

    Response listReports(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(crawlId);
            if (crawl == null) {
                return notFound("No such crawl: " + crawlId);
            }

            String out = "";
            Path bundle = crawl.path.resolve("crawl-bundle.zip");
            try (ZipFile zip = new ZipFile(bundle.toFile())) {
                for (ZipEntry entry : Collections.list(zip.entries())) {
                    out += entry.getName() + "\n";
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return response(out);
        }
    }
}
