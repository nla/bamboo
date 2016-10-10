package bamboo.crawl;

import static droute.Response.response;
import static droute.Route.GET;
import static droute.Route.routes;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
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

import bamboo.app.Bamboo;
import bamboo.task.Cdx;
import bamboo.task.Document;
import bamboo.task.TextExtractionException;
import bamboo.task.TextExtractor;
import bamboo.task.WarcUtils;
import bamboo.util.Parsing;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import droute.Handler;
import droute.Request;
import droute.Response;
import droute.Streamable;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class WarcsController {
    final Bamboo wa;
    public final Handler routes = routes(
            GET("/warcs/:id", this::serve, "id", "[0-9]+"),
            GET("/warcs/:id/cdx", this::showCdx, "id", "[0-9]+"),
            GET("/warcs/:id/text", this::showText, "id", "[0-9]+"),
            GET("/warcs/:id/details", this::details, "id", "[0-9]+"),
            GET("/warcs/:filename", this::serve, "filename", "[^/]+"),
            GET("/warcs/:filename/cdx", this::showCdx, "filename", "[^/]+"),
            GET("/warcs/:filename/cdx", this::showText, "filename", "[^/]+")

            );

    public WarcsController(Bamboo wa) {
        this.wa = wa;
    }

    Response render(String view, Object... model) {
        return Response.render("/" + getClass().getName().replaceFirst("\\.[^.]*$","").replace('.', '/') + "/views/" + view, model);
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
        if (request.param("id") != null) {
            long warcId = Long.parseLong(request.param("id"));
            return wa.warcs.get(warcId);
        } else if (request.param("filename") != null) {
            return wa.warcs.getByFilename(request.param("filename"));
        } else {
            throw new IllegalStateException("id or filename is required");
        }
    }

    Response serve(Request request) {
        return serve(request, findWarc(request));
    }

    private Response serve(Request request, Warc warc) {
        List<Range> ranges = Range.parseHeader(request.header("Range"), warc.getSize());
        try {
            if (ranges == null || ranges.isEmpty()) {
                return response(200, Files.newInputStream(warc.getPath()))
                        .withHeader("Content-Length", Long.toString(warc.getSize()))
                        .withHeader("Content-Type", "application/warc")
                        .withHeader("Content-Disposition", "filename=" + warc.getFilename())
                        .withHeader("Accept-Ranges", "bytes");
            } else if (ranges.size() == 1) {
                return singleRangeResponse(warc.getPath(), ranges.get(0));
            } else {
                return multipleRangeResponse(warc.getPath(), ranges);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Response singleRangeResponse(Path path, Range range) {
        return response(206, (Streamable)(OutputStream outStream) -> {
            WritableByteChannel out = Channels.newChannel(outStream);
            try (FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
                in.transferTo(range.start, range.length, out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).withHeader("Content-Range", range.toString())
                .withHeader("Content-Length", Long.toString(range.length))
                .withHeader("Content-Type", "application/warc");
    }

    private static final String boundary = "Te2akaimeeThe8eip5oh";

    private Response multipleRangeResponse(Path path, List<Range> ranges) throws IOException {
        return response(206, (Streamable) (OutputStream outStream) -> {
            WritableByteChannel out = Channels.newChannel(outStream);
            try (FileChannel in = FileChannel.open(path, StandardOpenOption.READ)) {
                for (Range range : ranges) {
                    out.write(asciiBuffer("--" + boundary + "\r\nContent-Type: application/warc\r\nContent-Range: " + range.toString() + "\r\n\r\n"));
                    in.transferTo(range.start, range.length, out);
                    out.write(asciiBuffer("\r\n"));
                }
                out.write(asciiBuffer("--" + boundary + "--\r\n"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).withHeader("Content-Type", "multipart/byteranges; boundary=" + boundary);
    }

    private static ByteBuffer asciiBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes(Charsets.UTF_8));
    }

    private Response showCdx(Request request) {
        Warc warc = findWarc(request);
        Path path = warc.getPath();
        String filename = path.getFileName().toString();
        return response(200, (Streamable) (OutputStream outStream) -> {
            Writer out = new BufferedWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
            Cdx.writeCdx(path, warc.getFilename(), out);
            out.flush();
        }).withHeader("Content-Type", "text/plain");
    }

    private static final Gson gson;
    static {
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String indent = System.getProperty("disableJsonIndent");
        if (indent != null && "true".equals(indent)) {
            gson = builder.create();
        } else {
            gson = builder.setPrettyPrinting().create();
        }
    }

    private static final TextExtractor extractor = new TextExtractor();

    private Response showText(Request request) {
        TextExtractor extractor = new TextExtractor();

        if (Parsing.parseLongOrDefault(request.queryParam("pdfbox"), 0) != 0) {
            extractor.setUsePdfBox(true);
        }

        if (Parsing.parseLongOrDefault(request.queryParam("boiled"), 0) != 0) {
            extractor.setBoilingEnabled(true);
        }

        if (Parsing.parseLongOrDefault(request.queryParam("tika"), 0) != 0) {
            extractor.setUseTika(true);
        }

        Warc warc = findWarc(request);
        return response(200, (Streamable) (OutputStream outStream) -> {
            try (ArchiveReader reader = WarcUtils.open(warc.getPath())) {
                JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
                writer.beginArray();
                for (ArchiveRecord record : reader) {
                    if (record.getHeader().getUrl() == null) continue;
                    try {
                        Document doc = extractor.extract(record);
                        gson.toJson(doc, Document.class, writer);
                    } catch (TextExtractionException e) {
                        continue; // skip it
                    }
                }
                writer.endArray();
                writer.flush();
            }
        }).withHeader("Content-Type", "application/json");
    }

    Response details(Request request) {
        Warc warc = findWarc(request);
        return render("warc.ftl",
                "warc", warc,
                "crawl", wa.crawls.get(warc.getCrawlId()),
                "state", wa.warcs.stateName(warc.getStateId()));
    }
}
