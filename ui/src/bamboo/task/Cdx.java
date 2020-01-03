package bamboo.task;

import bamboo.util.Urls;
import com.codepoetics.protonpack.StreamUtils;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang.StringUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bamboo.task.WarcUtils.cleanUrl;

public class Cdx {

    public static Stream<CdxRecord> records(ArchiveReader warcReader, String filename, long warcLength) {
        Stream<CdxRecord> stream = Stream.generate(new CdxRecordProducer(warcReader, filename, warcLength)::next);
        return StreamUtils.takeWhile(stream, (record) -> record != null);
    }

    public static void writeCdx(ArchiveReader warcReader, String filename, long warcLength, Writer out) throws IOException {
        records(warcReader, filename, warcLength).forEach(record -> {
            try {
                out.write(record.toCdxLine() + "\n");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    static class CdxRecordProducer {
        final static Pattern PANDORA_URL_MAP = Pattern.compile("^http://pandora\\.nla\\.gov\\.au/pan/([0-9]+/[0-9-]+)/url\\.map$");
        final static Pattern PANDORA_RECURSIVE_URL = Pattern.compile("^http://pandora\\.nla\\.gov\\.au/pan/([0-9]+/[0-9-]+)/pandora\\.nla\\.gov\\.au/pan/.*");

        private final PeekingIterator<ArchiveRecord> iterator;
        private final String filename;
        private final long warcLength;
        private Iterator<Alias> urlMapIterator = null;
        private boolean pandoraHacks;
        private Queue<String> panIndexPages = new ArrayDeque<>();

        CdxRecordProducer(ArchiveReader warc, String filename, long warcLength) {
            iterator = Iterators.peekingIterator(warc.iterator());
            this.filename = filename;
            this.warcLength = warcLength;

            if (filename.startsWith("nla.arc-")) {
                pandoraHacks = true;
            }
        }

        public CdxRecord next() {
            try {
                if (pandoraHacks) {
                    if (urlMapIterator != null && urlMapIterator.hasNext()) {
                        return urlMapIterator.next();
                    }
                }

                while (iterator.hasNext()) {
                    ArchiveRecord record = iterator.next();

                    if (pandoraHacks) {
                        String url = record.getHeader().getUrl();

                        // Generate alias records from PANDORA url.map if we encounter it.
                        if (url != null) {
                            Matcher m = PANDORA_URL_MAP.matcher(url);
                            if (m.matches()) {
                                urlMapIterator = parseUrlMap(record, m.group(1)).iterator();
                                return next();
                            }

                            // skip recursive PANDORA URLs where we've accidentally archived our own archive as they
                            // break delivery. eg nla.arc-24825-20140805-0714
                            if (PANDORA_RECURSIVE_URL.matcher(url).matches()) {
                                return next();
                            }

                            // Save PANDORA index pages for extra aliasing at the end.
                            if (urlMapIterator == null && url.endsWith("/index.html")) {
                                panIndexPages.add(url);
                            }
                        }
                    }

                    Capture capture = Capture.parseWarcRecord(filename, record);
                    if (capture != null) {
                        long endOfRecord = iterator.hasNext() ? iterator.peek().getHeader().getOffset() : warcLength;
                        capture.compressedLength = endOfRecord - capture.offset;
                        return capture;
                    }
                }

                if (pandoraHacks) {
                    /*
                     * XXX: PANDORA used to be served as static files and so relies
                     * on a web server automatically resolving /foo/ to /foo/index.html
                     * Sometimes url.map will take care of this mapping but early
                     * instances have no url.map. So if there's no url.map generate
                     * an alias records for every .../index.html to .../
                     */
                    if (urlMapIterator == null && !panIndexPages.isEmpty()) {
                        String url = panIndexPages.remove();
                        String dirname = url.substring(0, url.length() - "index.html".length());
                        return new Alias(dirname, url);
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

    static List<Alias> parseUrlMap(InputStream in, String piAndDate) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII));
        List<Alias> aliases = primaryAliases(reader, piAndDate);
        aliases.addAll(secondaryAliases(aliases, piAndDate));
        return aliases;
    }

    /**
     * Sometimes HTTrack's url-rewriting is only partially successful, often due to JavaScript constructing
     * URLs.  So we build a second set of aliases that include original URL but relative to the PANDORA instance.
     *
     * We don't include a secondary alias if there's already a primary alias covering it.
     */
    static List<Alias> secondaryAliases(List<Alias> primaryAliases, String piAndDate) {
        Set<String> primaryUrls = new HashSet<>();
        for (Alias alias : primaryAliases) {
            primaryUrls.add(alias.alias);
        }

        List<Alias> out = new ArrayList<>();
        for (Alias alias : primaryAliases) {
            String secondaryUrl = "http://pandora.nla.gov.au/pan/" + piAndDate + "/" + Urls.removeScheme(alias.target);
            if (!primaryUrls.contains(secondaryUrl) && isUrlSane(secondaryUrl)) {
                out.add(new Alias(secondaryUrl, alias.target));
            }
        }
        return out;
    }

    public static boolean isUrlSane(String url) {
        try {
            URI targetUri = new URI(url);
            String host = targetUri.getHost();
            if (host == null || !host.contains(".")) {
                return false;
            }
        } catch (URISyntaxException e) {
            return false;
        }
        return true;
    }

    static List<Alias> primaryAliases(BufferedReader reader, String piAndDate) throws IOException {
        List<Alias> aliases = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            Alias alias = parseUrlMapLine(line, piAndDate);
            if (alias != null && alias.isSane()) {
                aliases.add(alias);
            }
        }
        return aliases;
    }

    /**
     * Parses a line from a PANDORA url.map file and returns a list of corresponding aliases.
     */
    static Alias parseUrlMapLine(String line, String piAndDate) {
        String[] fields = line.trim().split("\\^\\^");
        if (fields.length < 2) return null;
        String targetUrl = Urls.addImplicitScheme(fields[0]);
        String instanceBaseUrl = "http://pandora.nla.gov.au/pan/" + piAndDate + "/";
        String aliasUrl = instanceBaseUrl + cleanHttrackPath(fields[1], piAndDate);
        return new Alias(aliasUrl, targetUrl);
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
            return "@alias " + cleanUrl(alias) + " " + cleanUrl(target);
        }

        public boolean isSane() {
            return isUrlSane(alias) && isUrlSane(target);
        }
    }

    public static class Capture implements CdxRecord {
        public String contentType;
        public int status;
        public String location;
        public String date;
        public String url;
        public long compressedLength;
        public long offset;
        public String filename;
        public String digest;

        public String toCdxLine() {
            return String.join(" ", "-", date, url, optional(contentType),
                    status == -1 ? "-" : Integer.toString(status), optional(digest),
                    optional(location), "-", Long.toString(compressedLength),
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

            if (WarcUtils.isResponseRecord(header)) {
                capture.url = WarcUtils.getCleanUrl(header);
                HttpHeader http = HttpHeader.parse(record, capture.url);
                if (http == null) {
                    return null;
                }
                capture.contentType = HttpHeader.cleanContentType(http.contentType);
                capture.status = http.status;
                capture.location = http.location;
            } else if (WarcUtils.isResourceRecord(header) ||
                    (WarcUtils.isMetadataRecord(header) && header.getUrl().startsWith("youtube-dl:"))) {
                capture.url = WarcUtils.getCleanUrl(header);
                capture.contentType = header.getMimetype();
                capture.status = 200;
                capture.location = null;
            } else {
                return null;
            }

            capture.date = WarcUtils.getArcDate(header);
            capture.offset = header.getOffset();
            capture.filename = filename;
            capture.digest = WarcUtils.getOrCalcDigest(record);

            return capture;
        }
    }
}
