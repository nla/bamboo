package bamboo.task;

import bamboo.util.Urls;
import com.codepoetics.protonpack.StreamUtils;
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
                out.write(record.toCdxLine());
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
                        String instancePath = m.group(1);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(record, StandardCharsets.US_ASCII));
                        urlMapIterator = reader.lines().map((line) -> parseUrlMapLine(instancePath, line)).iterator();
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

    static Alias parseUrlMapLine(String instancePath, String line) {
        String[] fields = line.trim().split("\\^\\^");
        Alias alias = new Alias();
        alias.target = Urls.addImplicitScheme(fields[0]);
        String httrackPath = fields[1];

        String legacyInstancePath = instancePath.replace("-0000", "");
        String rewrittenUrl = "http://pandora.nla.gov.au/pan";
        if (httrackPath.startsWith("/" + instancePath + "/")) {
            rewrittenUrl += httrackPath;
        } else if (httrackPath.startsWith(instancePath + "/")) {
            rewrittenUrl += "/" + httrackPath;
        } else if (httrackPath.startsWith("/" + legacyInstancePath + "/")){
            rewrittenUrl += "/" + instancePath + "/" + httrackPath.substring(legacyInstancePath.length() + 2);
        } else {
            rewrittenUrl += "/" + instancePath + "/" + httrackPath;
        }
        alias.alias = rewrittenUrl;
        return alias;
    }

    public static class Alias implements CdxRecord {
        public String alias;
        public String target;

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
