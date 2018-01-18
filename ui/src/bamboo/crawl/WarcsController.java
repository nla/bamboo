package bamboo.crawl;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import bamboo.app.Bamboo;
import bamboo.task.*;
import bamboo.util.Freemarker;
import bamboo.util.Parsing;
import bamboo.util.SurtFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import net.didion.jwnl.data.Exc;
import org.apache.log4j.lf5.util.StreamUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.url.SURT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.Request;
import spark.Response;
import spark.utils.GzipUtils;

import static java.nio.charset.StandardCharsets.*;

public class WarcsController {
    private static final Logger log = LoggerFactory.getLogger(WarcsController.class);

    final Bamboo wa;
    public void routes() {
        Spark.get("/warcs/:id", this::serve);
        Spark.get("/warcs/:id/cdx", this::showCdx);
        Spark.get("/warcs/:id/text", this::showText);
        Spark.get("/warcs/:id/details", this::details);
    }

    public WarcsController(Bamboo wa) {
        this.wa = wa;
    }

    String render(Request request, String view, Object... model) {
        return Freemarker.render(request, "bamboo/crawl/views/" + view, model);
    }

    static class Range {
        static final Pattern BYTES_SPEC_PATTERN = Pattern.compile("([0-9]+)?-([0-9]+)?");

        final long start, length, total;

        public Range(long start, long length, long total) {
            this.start = start;
            this.length = length;
            this.total = total;
        }

        public String toString() {
            return String.format("%d-%d/%d", start, start + length - 1, total);
        }

        static List<Range> parseHeader(String headerValue, long fileSize) {
            if (headerValue == null || !headerValue.startsWith("bytes=") || headerValue.equals("bytes=")) {
                return null;
            }
            List<Range> ranges = new ArrayList<>();
            String[] specs = headerValue.substring("bytes=".length()).split(",");
            for (String spec: specs) {
                Range range = parseByteRange(spec, fileSize);
                if (range == null) { // whole file
                    return null;
                }
                ranges.add(range);
            }
            return ranges;
        }

        static Range parseByteRange(String spec, long fileSize)  {
            Matcher m = BYTES_SPEC_PATTERN.matcher(spec);
            if (m.matches()) {
                String startText = m.group(1);
                String endText = m.group(2);
                if (startText != null) {
                    long start = Long.parseLong(startText);
                    long end = endText == null ? fileSize : Long.parseLong(endText);
                    return new Range(start, end - start + 1, fileSize);
                } else if (endText != null) {
                    long tail = Long.parseLong(endText);
                    if (tail >= fileSize) {
                        return null;
                    }
                    return new Range(fileSize - tail, tail, fileSize);
                }
            }
            throw new NumberFormatException("Bad byte range: " + spec);
        }
    }

    Warc findWarc(Request request) {
        String id = request.params(":id");
        try {
            long warcId = Long.parseLong(id);
            return wa.warcs.get(warcId);
        } catch (NumberFormatException e) {
            return wa.warcs.getByFilename(id);

        }
    }

    String serve(Request request, Response response) {
        return serve(request, response, findWarc(request));
    }

    private String serve(Request request, Response response, Warc warc) {
        List<Range> ranges = Range.parseHeader(request.headers("Range"), warc.getSize());
        try {
            if (ranges == null || ranges.isEmpty()) {
                response.type("application/warc");
                response.header("Content-Length", Long.toString(warc.getSize()));
                response.header("Content-Disposition", "filename=" + warc.getFilename());
                response.header("Accept-Ranges", "bytes");

                try (InputStream src = Files.newInputStream(warc.getPath());
                     OutputStream dst = response.raw().getOutputStream()) {
                    byte[] buf = new byte[16384];
                    for (int n = src.read(buf); n >= 0; n = src.read(buf)) {
                        dst.write(buf, 0, n);
                    }
                }

                return "";
            } else if (ranges.size() == 1) {
                return singleRangeResponse(response, warc.getPath(), ranges.get(0));
            } else {
                return multipleRangeResponse(response, warc.getPath(), ranges);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String singleRangeResponse(Response response, Path path, Range range) throws IOException {
        response.status(206);
        response.type("application/warc");
        response.header("Content-Range", range.toString());
        response.header("Content-Length", Long.toString(range.length));
        try (WritableByteChannel out = Channels.newChannel(response.raw().getOutputStream());
             FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
            in.transferTo(range.start, range.length, out);
        }
        return "";
    }

    private static final String boundary = "Te2akaimeeThe8eip5oh";

    private String multipleRangeResponse(Response response, Path path, List<Range> ranges) throws IOException {
        response.status(206);
        response.type("multipart/byteranges; boundary=" + boundary);
        try (WritableByteChannel out = Channels.newChannel(response.raw().getOutputStream());
             FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
            for (Range range : ranges) {
                out.write(asciiBuffer("--" + boundary + "\r\nContent-Type: application/warc\r\nContent-Range: " + range.toString() + "\r\n\r\n"));
                in.transferTo(range.start, range.length, out);
                out.write(asciiBuffer("\r\n"));
            }
            out.write(asciiBuffer("--" + boundary + "--\r\n"));
            return "";
        }
    }

    private static ByteBuffer asciiBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes(Charsets.UTF_8));
    }

    private String showCdx(Request request, Response response) throws IOException {
        Warc warc = findWarc(request);
        Path path = warc.getPath();
        String filename = path.getFileName().toString();
        response.type("text/plain");
        try (Writer out = new BufferedWriter(new OutputStreamWriter(response.raw().getOutputStream(), UTF_8))) {
            Cdx.writeCdx(path, warc.getFilename(), out);
            out.flush();
        }
        return "";
    }

    @VisibleForTesting
    static final Gson gson;
    static {
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        String indent = System.getProperty("disableJsonIndent");
        if (indent != null && "true".equals(indent)) {
            gson = builder.create();
        } else {
            gson = builder.setPrettyPrinting().create();
        }
    }

    private static final TextExtractor extractor = new TextExtractor();

    static class CollectionMatcher {
        private final SurtFilter filter;
        final CollectionInfo info;

        public CollectionMatcher(CollectionWithFilters collectionWithFilters) {
            this.info = new CollectionInfo();
            info.setId(collectionWithFilters.getId());
            info.setName(collectionWithFilters.getName());
            this.filter = new SurtFilter(collectionWithFilters.urlFilters);
        }

        public boolean matches(String surt) {
            return this.filter.accepts(surt);
        }
    }

    private String showText(Request request, Response response) throws IOException {
        TextExtractor extractor = new TextExtractor();

        if (Parsing.parseLongOrDefault(request.queryParams("pdfbox"), 0) != 0) {
            extractor.setUsePdfBox(true);
        }

        if (Parsing.parseLongOrDefault(request.queryParams("boiled"), 0) != 0) {
            extractor.setBoilingEnabled(true);
        }

        if (Parsing.parseLongOrDefault(request.queryParams("tika"), 0) != 0) {
            extractor.setUseTika(true);
        }

        Warc warc = findWarc(request);
        Crawl crawl = wa.crawls.get(warc.getCrawlId());
        List<CollectionMatcher> collections = wa.collections.findByCrawlSeriesId(crawl.getCrawlSeriesId())
                .stream().map(CollectionMatcher::new).collect(Collectors.toList());

        response.type("application/json");
        OutputStream outputStream = GzipUtils.checkAndWrap(request.raw(), response.raw(), false);
        OutputStreamWriter streamWriter = new OutputStreamWriter(outputStream, UTF_8);
        String url = null;
        try (ArchiveReader reader = WarcUtils.open(warc.getPath())) {
            JsonWriter writer = gson.newJsonWriter(streamWriter);
            writer.beginArray();
            for (ArchiveRecord record : reader) {
                url = record.getHeader().getUrl();
                if (url == null) continue;
                try {
                    Document doc = extractor.extract(record);

                    // add collections info
                    String surt = SURT.toSURT(doc.getUrl());
                    List<CollectionInfo> docCollections = new ArrayList<>();
                    for (CollectionMatcher matcher: collections) {
                        if (matcher.matches(surt)) {
                            docCollections.add(matcher.info);
                        }
                    }
                    doc.setCollections(docCollections);

                    // return as JSON
                    gson.toJson(doc, Document.class, writer);
                } catch (TextExtractionException e) {
                    continue; // skip it
                }
            }
            writer.endArray();
            writer.flush();
            return "";
        } catch (Exception | StackOverflowError e) {
            String message = "Text extraction failed. warcId=" + warc.getId() + " path=" + warc.getPath() + " recordUrl=" + url;
            log.error(message, e);
            streamWriter.write("\n\n" + message + "\n");
            e.printStackTrace(new PrintWriter(streamWriter));
            return "";
        } finally {
            streamWriter.close(); // ensure output stream always closed to avoid gzip issues
        }
    }

    String details(Request request, Response response) {
        Warc warc = findWarc(request);
        return render(request, "warc.ftl",
                "warc", warc,
                "crawl", wa.crawls.get(warc.getCrawlId()),
                "state", wa.warcs.stateName(warc.getStateId()));
    }
}
