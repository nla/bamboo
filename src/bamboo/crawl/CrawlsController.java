package bamboo.crawl;

import bamboo.pandas.PandasInstance;
import bamboo.app.Bamboo;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import droute.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

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

    Response render(String view, Object... model) {
        return Response.render("/" + getClass().getName().replaceFirst("\\.[^.]*$","").replace('.', '/') + "/views/" + view, model);
    }

    Response index(Request request) {
        Pager<CrawlAndSeriesName> pager = bamboo.crawls.pager(Parsing.parseLongOrDefault(request.param("page"), 1));
        return render("crawls/index.ftl",
                "crawls", pager.items,
                "crawlsPager", pager);
    }

    Response show(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Crawl crawl = bamboo.crawls.get(id);
        CrawlStats stats = bamboo.crawls.stats(id);

        PandasInstance instance = null;
        if (crawl.getPandasInstanceId() != null && bamboo.pandas != null) {
            instance = bamboo.pandas.getInstance(crawl.getPandasInstanceId());
        }

        return render("crawls/show.ftl",
                "crawl", crawl,
                "series", bamboo.serieses.get(crawl.getCrawlSeriesId()),
                "warcsToBeCdxIndexed", stats.getWarcsToBeCdxIndexed(),
                "warcsToBeSolrIndexed", stats.getWarcsToBeSolrIndexed(),
                "corruptWarcs", stats.getCorruptWarcs(),
                "descriptionHtml", Markdown.render(crawl.getDescription(), request.uri()),
                "pandasInstance", instance
                );
    }

    Response edit(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Crawl crawl = bamboo.crawls.get(id);
        return render("crawls/edit.ftl",
                "crawl", crawl,
                "csrfToken", Csrf.token(request)
        );
    }

    Response update(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        bamboo.crawls.update(crawlId, request.formParam("name"), request.formParam("description"));
        return seeOther(request.contextUri().resolve("crawls/" + crawlId).toString());
    }

    Response listWarcs(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlId(Parsing.parseLongOrDefault(request.queryParam("page"), 1), id);
        return render("crawls/warcs.ftl",
                "crawl", crawl,
                "warcs", pager.items,
                "warcsPager", pager);
    }

    Response listCorruptWarcs(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlIdAndState(Parsing.parseLongOrDefault(request.queryParam("page"), 1), id, Warc.CDX_ERROR);
        return render("crawls/warcs.ftl",
                "titlePrefix", "Corrupt",
                "crawl", crawl,
                "warcs", pager.items,
                "warcsPager", pager);
    }

    Response downloadWarcs(Request request) {
        long crawlId = Long.parseLong(request.urlParam("id"));
        List<Warc> warcs = bamboo.warcs.findByCrawlId(crawlId);
        if (warcs.isEmpty()) {
            return response(404, "No warcs found");
        }
        return response((Streamable) out -> {
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                for (Warc warc : warcs) {
                    writeZipEntry(zip, "crawl-" + crawlId + "/" + warc.getFilename(), warc.getPath());
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
        long id = Long.parseLong(request.urlParam("id"));
        Crawl crawl = bamboo.crawls.get(id);
        String out = "";
        Path bundle = crawl.getPath().resolve("crawl-bundle.zip");
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
