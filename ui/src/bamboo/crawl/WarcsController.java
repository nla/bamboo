package bamboo.crawl;

import bamboo.app.Bamboo;
import bamboo.core.Streams;
import bamboo.task.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.io.output.TeeOutputStream;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.url.SURT;
import org.netpreserve.jwarc.WarcReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.server.ResponseStatusException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

@Controller
public class WarcsController {
    private static final Logger log = LoggerFactory.getLogger(WarcsController.class);

    final Bamboo wa;
    public final TextCache textCache;

    public WarcsController(Bamboo wa, @Autowired(required = false) TextCache textCache) {
        this.wa = Objects.requireNonNull(wa);
        this.textCache = textCache;
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
                    if (start >= fileSize) {
                        throw new ResponseStatusException(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE, "Start position past end of file");
                    }
                    end = Math.min(end, fileSize - 1);
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

    Warc findWarc(String id) {
        try {
            long warcId = Long.parseLong(id);
            return wa.warcs.get(warcId);
        } catch (NumberFormatException e) {
            return wa.warcs.getByFilename(id);
        }
    }

    @GetMapping("/warcs/{id}")
    public void serve(@PathVariable("id") String id,
                 @RequestHeader(value = "Range", required = false) String rangeHeader,
                 HttpServletRequest request, HttpServletResponse response) {
        Warc warc = findWarc(id);
        serveWarc(warc, rangeHeader, request, response);
    }


    @GetMapping("/crawls/{id}/warcs/{filename}")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'view')")
    public void serveInCrawl(@PathVariable("id") long crawlId, @PathVariable("filename") String filename,
                 @RequestHeader(value = "Range", required = false) String rangeHeader,
                 HttpServletRequest request,
                 HttpServletResponse response) {
        Warc warc = wa.warcs.getByCrawlIdAndFilename(crawlId, filename);
        serveWarc(warc, rangeHeader, request, response);
    }

    public void serveWarc(Warc warc, String rangeHeader, HttpServletRequest request, HttpServletResponse response) {
        List<Range> ranges = Range.parseHeader(rangeHeader, warc.getSize());
        try {
            if (ranges == null || ranges.isEmpty()) {
                response.setContentType("application/warc");
                response.setHeader("Content-Length", Long.toString(warc.getSize()));
                response.setHeader("Content-Disposition", "filename=" + warc.getFilename());
                response.setHeader("Accept-Ranges", "bytes");
                String sha256 = warc.getSha256();
                if (sha256 != null) {
                    response.setHeader("Digest", "sha-256=" + Base64.getEncoder().encodeToString(Hex.decode(sha256)));
                }
                if (request.getMethod().equals("HEAD")) return;

                try (InputStream src = wa.warcs.openStream(warc);
                     OutputStream dst = response.getOutputStream()) {
                    Streams.copy(src, dst);
                }
            } else if (ranges.size() == 1) {
                singleRangeResponse(request, response, warc, ranges.get(0));
            } else {
                multipleRangeResponse(request, response, warc, ranges);
            }
        } catch (IOException e) {
            if (e.getMessage().contains("Connection reset by peer") || e.getMessage().contains("Broken pipe")) {
                return;
            }
            throw new UncheckedIOException(e);
        }
    }

    private void singleRangeResponse(HttpServletRequest request, HttpServletResponse response, Warc warc, Range range) throws IOException {
        response.setStatus(206);
        response.setContentType("application/warc");
        response.setHeader("Content-Range", range.toString());
        response.setHeader("Content-Length", Long.toString(range.length));
        if (request.getMethod().equals("HEAD")) return;
        wa.warcs.copy(warc, response.getOutputStream(), range.start, range.length);
    }

    private static final String boundary = "Te2akaimeeThe8eip5oh";

    private void multipleRangeResponse(HttpServletRequest request, HttpServletResponse response, Warc warc, List<Range> ranges) throws IOException {
        response.setStatus(206);
        response.setContentType("multipart/byteranges; boundary=" + boundary);
        if (request.getMethod().equals("HEAD")) return;
        try (OutputStream out = response.getOutputStream()) {
            for (Range range : ranges) {
                out.write(("--" + boundary + "\r\nContent-Type: application/warc\r\nContent-Range: " + range.toString() + "\r\n\r\n").getBytes(US_ASCII));
                wa.warcs.copy(warc, out, range.start, range.length);
                out.write("\r\n".getBytes(US_ASCII));
            }
            out.write(("--" + boundary + "--\r\n").getBytes(US_ASCII));
        }
    }

    @GetMapping(value = "/warcs/{id}/cdx")
    public void showCdx(@PathVariable("id") String id, HttpServletResponse response) throws IOException {
        Warc warc = findWarc(id);
        response.setContentType("text/plain");
        PrintWriter out = new PrintWriter(response.getOutputStream(), false, UTF_8);
        try (WarcReader warcReader = new WarcReader(wa.warcs.openStream(warc))) {
            Cdx.buildIndex(warcReader, out, warc.getFilename());
        } catch (Exception e) {
            log.error("Unable to produce CDX for warc " + warc.getId(), e);
            e.printStackTrace(out);
        } finally {
            out.flush();
        }
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

    static class CollectionMatcher {
        final CollectionInfo info;

        public CollectionMatcher(Collection collection) {
            this.info = new CollectionInfo();
            info.setId(collection.getId());
            info.setName(collection.getName());
        }

        public boolean matches(String surt) {
            return true;
        }
    }

    private boolean serveTextFromCache(HttpServletRequest request, HttpServletResponse response, Warc warc, List<CollectionMatcher> collections) throws IOException {
        if (textCache == null) return false;
        Path file = textCache.find(warc.getId());
        if (file == null) return false;

        response.setContentType("application/json");
        try (JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(response.getOutputStream(), UTF_8));
             JsonReader reader = gson.newJsonReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(file), 8192), UTF_8))) {
            reader.beginArray();
            writer.beginArray();
            while (reader.hasNext()) {
                Document doc = gson.fromJson(reader, Document.class);
                populateCollectionInfo(collections, doc);
                gson.toJson(doc, Document.class, writer);
            }
            reader.endArray();
            writer.endArray();
            writer.flush();
            return true;
        } catch (JsonSyntaxException e) {
            log.error("Deleting corrupt cache entry {}", file);
            Files.deleteIfExists(file);
            // we can't meaningfully recover in this situation so bail and hope the client retries
            throw e;
        }
    }

    @GetMapping(value = "/warcs/{id}/text", produces = "application/json")
    public void showText(@PathVariable String id, HttpServletRequest request, HttpServletResponse response) throws IOException {

        Warc warc = findWarc(id);
        Crawl crawl = wa.crawls.get(warc.getCrawlId());
        List<CollectionMatcher> collections = wa.collections.findByCrawlSeriesId(crawl.getCrawlSeriesId())
                .stream().map(CollectionMatcher::new).collect(Collectors.toList());

        if (serveTextFromCache(request, response, warc, collections)) {
            return;
        }

        response.setContentType("application/json");

        OutputStream out = response.getOutputStream();
        Path cachePath = null;
        Path tmpCachePath = null;
        OutputStream cacheStream = null;
        if (textCache != null) {
            cachePath = textCache.entryPath(warc.getId());
            tmpCachePath = Paths.get(cachePath.toString() + ".tmp");
            Files.createDirectories(tmpCachePath.getParent());
            cacheStream = new GZIPOutputStream(Files.newOutputStream(tmpCachePath, WRITE, CREATE, TRUNCATE_EXISTING), 8192);
            out = new TeeOutputStream(out, cacheStream);
        }

        OutputStreamWriter streamWriter = new OutputStreamWriter(out, UTF_8);
        String url = null;
        try (ArchiveReader reader = wa.warcs.openReader(warc)) {
            JsonWriter writer = gson.newJsonWriter(streamWriter);
            writer.beginArray();
            for (ArchiveRecord record : reader) {
                url = record.getHeader().getUrl();
                if (url == null) continue;
                try {
                    Document doc = wa.textExtractor.extract(record);
                    populateCollectionInfo(collections, doc);
                    gson.toJson(doc, Document.class, writer);
                } catch (TextExtractionException e) {
                    continue; // skip it
                }
            }
            writer.endArray();
            writer.flush();

            if (tmpCachePath != null) {
                cacheStream.close();
                cacheStream = null;
                Files.move(tmpCachePath, cachePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception | StackOverflowError e) {
            String message = "Text extraction failed. warcId=" + warc.getId() + " path=" + warc.getPath() + " recordUrl=" + url;
            log.error(message, e);
            streamWriter.write("\n\n" + message + "\n");
            e.printStackTrace(new PrintWriter(streamWriter));
        } finally {
            streamWriter.close(); // ensure output stream always closed to avoid gzip issues
            if (cacheStream != null) cacheStream.close();
            if (tmpCachePath != null) Files.deleteIfExists(tmpCachePath);
        }
    }

    private void populateCollectionInfo(List<CollectionMatcher> collections, Document doc) {
        // add collections info
        String surt = SURT.toSURT(doc.getUrl());
        List<CollectionInfo> docCollections = new ArrayList<>();
        for (CollectionMatcher matcher: collections) {
            if (matcher.matches(surt)) {
                docCollections.add(matcher.info);
            }
        }
        doc.setCollections(docCollections);
    }

    @GetMapping(value = "/warcs/{id}/details")
    String details(@PathVariable String id, Model model) {
        Warc warc = findWarc(id);
        model.addAttribute("warc", warc);
        model.addAttribute("crawl", wa.crawls.get(warc.getCrawlId()));
        model.addAttribute("state", wa.warcs.stateName(warc.getStateId()));
        return "warc";
    }

    @PostMapping(value = "/warcs/{warcId}/reindex")
    @ResponseBody
    @PreAuthorize("hasPermission(#warcId, 'Warc', 'edit')")
    String reindex(@PathVariable long warcId, Model model) throws IOException {
        Warc warc = wa.warcs.get(warcId);
        RecordStats stats = wa.cdxIndexer.indexWarc(warc);
        return "CDX indexed " + stats.getRecords() + " records";
    }

    @DeleteMapping("/warcs/{warcId}")
    @PreAuthorize("hasPermission(#warcId, 'Warc', 'edit')")
    public void deleteWarc(@PathVariable("warcId") long warcId) throws IOException {
        Warc warc = wa.warcs.get(warcId);
        if (warc.getStateId() == Warc.DELETED) return;
        wa.cdxIndexer.deindexWarc(warc);
    }

    @PostMapping("/warcs/{warcId}/delete")
    @PreAuthorize("hasPermission(#warcId, 'Warc', 'edit')")
    public String deleteWarcWithPost(@PathVariable("warcId") long warcId) throws IOException {
        deleteWarc(warcId);
        return "redirect:/warcs/" + warcId + "/details";
    }

    @PostMapping("/warcs/{warcId}/move-to-blob-storage")
    @ResponseBody
    @PreAuthorize("hasRole('SYSADMIN')")
    public String moveToBlobStorage(
            @PathVariable("warcId") long warcId,
            @RequestParam(value = "deleteOriginal", defaultValue = "true") boolean deleteOriginal) throws IOException {
        return wa.warcs.moveToBlobStorage(warcId, deleteOriginal);
    }
}
