package bamboo.task;

import bamboo.util.Urls;
import com.codepoetics.protonpack.StreamUtils;
import org.apache.commons.lang.StringUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Cdx {

    public static Stream<CdxRecord> records(ArchiveReader warcReader) {
        Stream<CdxRecord> stream = Stream.generate(new CdxRecordProducer(warcReader)::next);
        return StreamUtils.takeWhile(stream, (record) -> record != null);
    }

    public static void writeCdx(Path warc, Writer out) throws IOException {
        records(Warcs.open(warc)).forEach(record -> {
            try {
                out.write(record.toCdxLine() + "\n");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    static class CdxRecordProducer {
        final static Pattern PANDORA_URL_MAP = Pattern.compile("^http://pandora.nla.gov.au/pan/([0-9]+/[0-9-]+)/url.map$");

        private final ArchiveReader warc;
        private final Iterator<ArchiveRecord> iterator;
        private Iterator<Alias> urlMapIterator = null;

        CdxRecordProducer(ArchiveReader warc) {
            this.warc = warc;
            iterator = warc.iterator();
        }

        public CdxRecord next() {
            try {
                if (urlMapIterator != null && urlMapIterator.hasNext()) {
                    return urlMapIterator.next();
                }
                while (iterator.hasNext()) {
                    ArchiveRecord record = iterator.next();
                    Matcher m = PANDORA_URL_MAP.matcher(record.getHeader().getUrl());
                    if (m.matches()) {
                        String piAndDate = m.group(1);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(record, StandardCharsets.US_ASCII));
                        urlMapIterator = reader.lines().flatMap((line) -> parseUrlMapLine(line, piAndDate)).iterator();
                        return next();
                    } else {
                        Capture capture = Capture.parseWarcRecord(warc.getFileName(), record);
                        if (capture != null) {
                            return capture;
                        }
                    }
                }
                return null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public interface CdxRecord {
        String toCdxLine();
    }

    /**
     * Parses a line from a PANDORA url.map file and returns a list of corresponding aliases.
     *
     * Sometimes HTTrack's url-rewriting is only partially successful, often due to JavaScript constructing
     * URLs.  So we return two aliases, one using HTTrack's rewritten URL and the other using the original URL
     * but relative to the PANDORA instance.
     */
    static Stream<Alias> parseUrlMapLine(String line, String piAndDate) {
        String[] fields = line.trim().split("\\^\\^");
        String target = Urls.addImplicitScheme(fields[0]);
        String instanceBaseUrl = "http://pandora.nla.gov.au/pan/" + piAndDate + "/";
        String alias1 = instanceBaseUrl + cleanHttrackPath(fields[1], piAndDate);
        String alias2 = instanceBaseUrl + Urls.removeScheme(target);
        if (alias1.equals(alias2)) {
            return Stream.of(new Alias(alias1, target));
        } else {
            return Stream.of(new Alias(alias1, target), new Alias(alias2, target));
        }
    }

    /**
     * Strips the pi and instance date from a PANDORA path if present.
     */
    static String cleanHttrackPath(String path, String piAndDate) {
        path = StringUtils.removeStart(path, "/");
        String piAndLegacyDate = StringUtils.removeEnd(piAndDate, "-0000");
        if (path.startsWith(piAndDate + "/")) {
            return path.substring(piAndDate.length() + 1); // 1234/20010101-1234/(example.org/index.html)
        } else if (path.startsWith(piAndLegacyDate + "/")){
            return path.substring(piAndLegacyDate.length() + 1); // 1234/20010101/(example.org/index.html)
        } else {
            return path; // (example.org/index.html)
        }
    }

    public static class Alias implements CdxRecord {
        public String alias;
        public String target;

        public Alias(String alias, String target) {
            this.alias = alias;
            this.target = target;
        }

        @Override
        public String toCdxLine() {
            return "@alias " + alias + " " + target;
        }
    }

    public static class Capture implements CdxRecord {
        public String contentType;
        public int status;
        public String location;
        public String date;
        public String url;
        public long contentLength;
        public long offset;
        public String filename;
        public String digest;

        public String toCdxLine() {
            return String.join(" ", "-", date, url, optional(contentType),
                    status == -1 ? "-" : Integer.toString(status), optional(digest),
                    optional(location), "-", Long.toString(contentLength),
                    Long.toString(offset), filename);
        }

        private static String optional(String s) {
            if (s == null) {
                return "-";
            }
            return s.replace(" ", "%20").replace("\n", "%0A").replace("\r", "%0D");
        }

        static Capture parseWarcRecord(String filename, ArchiveRecord record) throws IOException {
            ArchiveRecordHeader header = record.getHeader();

            Capture capture = new Capture();
            capture.url = Warcs.getCleanUrl(header);

            if (Warcs.isResponseRecord(header)) {
                HttpHeader http = HttpHeader.parse(record, capture.url);
                if (http == null) {
                    return null;
                }
                capture.contentType = http.getCleanContentType();
                capture.status = http.status;
                capture.location = http.location;
            } else if (Warcs.isResourceRecord(header)) {
                capture.contentType = header.getMimetype();
                capture.status = 200;
                capture.location = null;
            } else {
                return null;
            }

            capture.date = Warcs.getArcDate(header);
            capture.contentLength = header.getContentLength();
            capture.offset = header.getOffset();
            capture.filename = filename;
            capture.digest = Warcs.getOrCalcDigest(record);
            return capture;
        }
    }
}
