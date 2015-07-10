package bamboo.web;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.task.CdxIndexer;
import com.google.common.base.Charsets;
import droute.Handler;
import droute.Request;
import droute.Response;
import droute.Streamable;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;

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

import static droute.Response.render;
import static droute.Response.response;
import static droute.Route.GET;
import static droute.Route.routes;

class WarcsController {
    final Bamboo bamboo;
    final Handler routes = routes(
            GET("/warcs/:id", this::serve, "id", "[0-9]+"),
            GET("/warcs/:id/cdx", this::showCdx, "id", "[0-9]+"),
            GET("/warcs/:id/details", this::details, "id", "[0-9]+"),
            GET("/warcs/:filename", this::serve, "filename", "[^/]+"),
            GET("/warcs/:filename/cdx", this::showCdx, "filename", "[^/]+")
            );

    WarcsController(Bamboo bamboo) {
        this.bamboo = bamboo;
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

    Db.Warc findWarc(Request request) {
        Db.Warc warc = null;
        try (Db db = bamboo.dbPool.take()) {
            if (request.param("id") != null) {
                long warcId = Long.parseLong(request.param("id"));
                warc = db.findWarc(warcId);
            } else if (request.param("filename") != null) {
                warc = db.findWarcByFilename(request.param("filename"));
            }
        }
        if (warc == null) {
            throw new RuntimeException("No such warc file");
        }
        return warc;
    }

    Response serve(Request request) {
        return serve(request, findWarc(request));
    }

    private Response serve(Request request, Db.Warc warc) {
        List<Range> ranges = Range.parseHeader(request.header("Range"), warc.size);
        try {
            if (ranges == null || ranges.isEmpty()) {
                return response(200, Files.newInputStream(warc.path))
                        .withHeader("Content-Length", Long.toString(warc.size))
                        .withHeader("Content-Type", "application/warc")
                        .withHeader("Content-Disposition", "filename=" + warc.filename)
                        .withHeader("Accept-Ranges", "bytes");
            } else if (ranges.size() == 1) {
                return singleRangeResponse(warc.path, ranges.get(0));
            } else {
                return multipleRangeResponse(warc.path, ranges);
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
    private static ByteBuffer CRLF = asciiBuffer("\r\n");

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
        Db.Warc warc = findWarc(request);
        Path path = warc.path;
        String filename = path.getFileName().toString();
        return response(200, (Streamable) (OutputStream outStream) -> {
            Writer out = new BufferedWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
            try (ArchiveReader reader = ArchiveReaderFactory.get(path.toFile())) {
                for (ArchiveRecord record : reader) {
                    String line = CdxIndexer.formatCdxLine(filename, record);
                    if (line != null) {
                        out.write(line);
                    }
                }
            } catch (Exception e) {
                out.write("\n\nError reading " + path + "\n");
                e.printStackTrace(new PrintWriter(out));
            } finally {
                out.flush();
            }
        }).withHeader("Content-Type", "text/plain");
    }

    Response details(Request request) {
        Db.Warc warc = findWarc(request);
        try (Db db = bamboo.dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(warc.crawlId);
            return render("warc.ftl",
                    "warc", warc,
                    "crawl", crawl);
        }
    }
}
