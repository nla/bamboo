package bamboo.crawl;

import bamboo.pandas.PandasInstance;
import bamboo.app.Bamboo;
import bamboo.util.Csrf;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static bamboo.util.Freemarker.render;


public class CrawlsController {
    final Bamboo bamboo;

    public void routes() {
        Spark.get("/crawls", this::index);
        Spark.get("/crawls/:id", this::show);
        Spark.get("/crawls/:id/edit", this::edit);
        Spark.post("/crawls/:id/edit", this::update);
        Spark.get("/crawls/:id/warcs", this::listWarcs);
        Spark.get("/crawls/:id/warcs/download", this::downloadWarcs);
        Spark.get("/crawls/:id/warcs/corrupt", this::listCorruptWarcs);
        Spark.get("/crawls/:id/reports", this::listReports);
    }

    public CrawlsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    String index(Request request, Response response) {
        Pager<CrawlAndSeriesName> pager = bamboo.crawls.pager(Parsing.parseLongOrDefault(request.queryParams("page"), 1));
        return render(request, "bamboo/crawl/views/crawls/index.ftl",
                "crawls", pager.items,
                "crawlsPager", pager);
    }

    String show(Request request, Response responses) {
        long id = Long.parseLong(request.params(":id"));
        Crawl crawl = bamboo.crawls.get(id);
        CrawlStats stats = bamboo.crawls.stats(id);

        PandasInstance instance = null;
        if (crawl.getPandasInstanceId() != null && bamboo.pandas != null) {
            instance = bamboo.pandas.getInstance(crawl.getPandasInstanceId());
        }

        return render(request, "bamboo/crawl/views/crawls/show.ftl",
                "crawl", crawl,
                "series", bamboo.serieses.get(crawl.getCrawlSeriesId()),
                "warcsToBeCdxIndexed", stats.getWarcsToBeCdxIndexed(),
                "corruptWarcs", stats.getCorruptWarcs(),
                "descriptionHtml", Markdown.render(crawl.getDescription(), request.uri()),
                "pandasInstance", instance
                );
    }

    String edit(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Crawl crawl = bamboo.crawls.get(id);
        return render(request, "bamboo/crawl/views/crawls/edit.ftl",
                "crawl", crawl,
                "csrfToken", Csrf.token(request)
        );
    }

    String update(Request request, Response response) {
        long crawlId = Long.parseLong(request.params(":id"));
        bamboo.crawls.update(crawlId, request.queryParams("name"), request.queryParams("description"));
        response.redirect(request.contextPath() + "/crawls/" + crawlId, 303);
        return "";
    }

    String listWarcs(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlId(Parsing.parseLongOrDefault(request.queryParams("page"), 1), id);

        if ("ids".equals(request.queryParams("format"))) {
            response.type("text/plain");
            response.header("Total", String.valueOf(pager.totalItems));
            StringBuilder sb = new StringBuilder();
            for (Warc warc: pager.items) {
                sb.append(warc.getId());
                sb.append('\n');
            }
            return sb.toString();
        }

        return render(request, "bamboo/crawl/views/crawls/warcs.ftl",
                "crawl", crawl,
                "warcs", pager.items,
                "warcsPager", pager);
    }

    String listCorruptWarcs(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlIdAndState(Parsing.parseLongOrDefault(request.queryParams("page"), 1), id, Warc.CDX_ERROR);
        return render(request, "crawls/warcs.ftl",
                "titlePrefix", "Corrupt",
                "crawl", crawl,
                "warcs", pager.items,
                "warcsPager", pager);
    }

    String downloadWarcs(Request request, Response response) throws IOException {
        long crawlId = Long.parseLong(request.params(":id"));
        List<Warc> warcs = bamboo.warcs.findByCrawlId(crawlId);
        if (warcs.isEmpty()) {
            throw Spark.halt(404, "No warcs found");
        }

        response.type("application/zip");
        response.header("Content-Disposition", "attachment; filename=crawl-" + crawlId + ".zip");

        try (ZipOutputStream zip = new ZipOutputStream(response.raw().getOutputStream())) {
            for (Warc warc : warcs) {
                writeZipEntry(zip, "crawl-" + crawlId + "/" + warc.getFilename(), warc.getPath());
            }
        }
        return "";
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

    String listReports(Request request, Response response) {
        long id = Long.parseLong(request.params(":id"));
        Crawl crawl = bamboo.crawls.get(id);
        StringBuilder out = new StringBuilder();
        Path bundle = crawl.getPath().resolve("crawl-bundle.zip");
        try (ZipFile zip = new ZipFile(bundle.toFile())) {
            for (ZipEntry entry : Collections.list(zip.entries())) {
                out.append(entry.getName()).append("\n");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        response.type("text/plain");
        return out.toString();
    }
}
