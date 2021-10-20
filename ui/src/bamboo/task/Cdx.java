package bamboo.task;

import bamboo.crawl.RecordStats;
import bamboo.util.Urls;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.netpreserve.jwarc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bamboo.task.WarcUtils.cleanUrl;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.ZoneOffset.UTC;

public class Cdx {
    final static Pattern PANDORA_URL_MAP = Pattern.compile("^http://pandora\\.nla\\.gov\\.au/pan/([0-9]+/[0-9-]+)/url\\.map$");
    final static Pattern PANDORA_RECURSIVE_URL = Pattern.compile("^http://pandora\\.nla\\.gov\\.au/pan/([0-9]+/[0-9-]+)/pandora\\.nla\\.gov\\.au/pan/.*");

    private static final MediaType JSON = MediaType.parse("application/json");
    private static final MediaType FORM_URLENCODED = MediaType.parse("application/x-www-form-urlencoded");
    private static final DateTimeFormatter ARC_DATE = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC);
    private static final Instant YEAR1990 = Instant.ofEpochMilli(631152000L);
    private static final Logger log = LoggerFactory.getLogger(Cdx.class);

    public static void main(String[] args) throws IOException {
        for (String arg : args) {
            Path path = Paths.get(arg);
            try (WarcReader reader = new WarcReader(path)) {
                PrintWriter out = new PrintWriter(System.out);
                buildIndex(reader, out, path.getFileName().toString());
                out.flush();
            }
        }
    }


    public static RecordStats buildIndex(WarcReader reader, PrintWriter out, String filename) throws IOException {
        return buildIndex(reader, out, filename, true);
    }

    public static RecordStats buildIndex(WarcReader reader, PrintWriter out, String filename, boolean allowAliases) throws IOException {
        RecordStats stats = new RecordStats();
        PandoraAliaser pandoraAliaser = allowAliases && filename.startsWith("nla.arc") ? new PandoraAliaser(out) : null;
        WarcRecord record = reader.next().orElse(null);
        while (record != null) {
            try {
                if ((record instanceof WarcResponse || record instanceof WarcResource) &&
                        ((WarcCaptureRecord) record).payload().isPresent()) {
                    if (pandoraAliaser != null) {
                        boolean skip = pandoraAliaser.accept((WarcCaptureRecord) record);
                        if (skip) {
                            record = reader.next().orElse(null);
                            continue;
                        }
                    }

                    WarcPayload payload = ((WarcCaptureRecord) record).payload().get();
                    MediaType type;
                    try {
                        type = payload.type().base();
                    } catch (IllegalArgumentException e) {
                        type = MediaType.OCTET_STREAM;
                    }
                    URI id = record.version().getProtocol().equals("ARC") ? null : record.id();
                    String url = ((WarcCaptureRecord) record).target();
                    Instant instant = record.date();
                    String date = ARC_DATE.format(instant);
                    int status = record instanceof WarcResponse ? ((WarcResponse) record).http().status() : 200;
                    String digest = payload.digest().map(WarcDigest::base32).orElse(null);
                    long position = reader.position();

                    if (instant.isBefore(YEAR1990)) {
                        // garbage. skip.
                        record = reader.next().orElse(null);
                        continue;
                    }

                    if (digest == null) {
                        digest = WarcUtils.calcDigest(payload.body().stream());
                    }

                    // advance to the next record so we can calculate the length
                    record = reader.next().orElse(null);
                    long length = reader.position() - position;

                    // check for a corresponding request record
                    while (record instanceof WarcCaptureRecord && ((WarcCaptureRecord) record).concurrentTo().contains(id)) {
                        if (record instanceof WarcRequest) {
                            HttpRequest httpRequest = ((WarcRequest) record).http();
                            if (httpRequest.method().equals("POST") || httpRequest.method().equals("PUT")) {
                                try {
                                    url += (url.contains("?") ? "&" : "?") + "__wb_method=" + httpRequest.method();
                                    MediaType baseContentType = httpRequest.contentType().base();
                                    if (baseContentType.equals(JSON)) {
                                        url += encodeJsonRequest(httpRequest.body().stream());
                                    } else if (baseContentType.equals(FORM_URLENCODED)) {
                                        url += "&" + new String(httpRequest.body().stream().readAllBytes(), ISO_8859_1);
                                    }
                                } catch (IllegalArgumentException e) {
                                    log.trace("Bad content-type: {}", httpRequest.headers().first("Content-Type"));
                                }
                                break;
                            }
                        }

                        record = reader.next().orElse(null);
                    }

                    out.printf("%s %s %s %s %d %s - - %d %d %s%n", url, date, url, type, status, digest, length, position, filename);
                    stats.update(length, Date.from(instant));
                } else {
                    record = reader.next().orElse(null);
                }
            } catch (ParsingException e) {
                log.warn("Bad record in " + filename + " (position " + reader.position() + ")", e);
                record = reader.next().orElse(null);
            }
        }

        if (pandoraAliaser != null) pandoraAliaser.finish();
        return stats;
    }


    static class PandoraAliaser {
        private final PrintWriter out;
        private List<String> indexPages = new ArrayList<>();

        PandoraAliaser(PrintWriter out) {
            this.out = out;
        }

        boolean accept(WarcCaptureRecord record) throws IOException {
            String url = record.target();

            if (!url.startsWith("http://pandora.nla.gov.au/pan/")) return false;

            // skip recursive PANDORA URLs where we've accidentally archived our own archive as they
            // break delivery. eg nla.arc-24825-20140805-0714
            if (PANDORA_RECURSIVE_URL.matcher(url).matches()) {
                return true;
            }

            Matcher matcher = PANDORA_URL_MAP.matcher(record.target());
            if (matcher.matches()) {
                parseUrlMap(record, matcher.group(1));
                indexPages = null;
                return true;
            }

            if (indexPages != null && url.startsWith("http://pandora.nla.gov.au/pan/") && url.endsWith("/index.html")) {
                indexPages.add(url);
            }

            return false;
        }

        public void finish() {
            /*
             * XXX: PANDORA used to be served as static files and so relies
             * on a web server automatically resolving /foo/ to /foo/index.html
             * Sometimes url.map will take care of this mapping but early
             * instances have no url.map. So if there's no url.map generate
             * an alias records for every .../index.html to .../
             */
            if (indexPages != null) {
                for (String url : indexPages) {
                    String dirname = url.substring(0, url.length() - "index.html".length());
                    writeAlias(dirname, url);
                }
            }
        }

        private void parseUrlMap(WarcCaptureRecord record, String piAndDate) throws IOException {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(record.payload().orElseThrow().body().stream(), US_ASCII));
            Set<String> aliases = new HashSet<>();
            List<String> targets = new ArrayList<>();
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                String[] fields = line.trim().split("\\^\\^");
                if (fields.length < 2) continue;
                String target = cleanUrl(Urls.addImplicitScheme(fields[0]));
                if (!isSaneUrl(target)) continue;
                String alias = cleanUrl(("http://pandora.nla.gov.au/pan/" + piAndDate + "/") +
                        cleanHttrackPath(fields[1], piAndDate));
                if (!isSaneUrl(alias)) continue;
                writeAlias(alias, target);
                aliases.add(alias);
                targets.add(target);
            }

            /*
             * Sometimes HTTrack's url-rewriting is only partially successful, often due to JavaScript constructing
             * URLs.  So we build a second set of aliases that include original URL but relative to the PANDORA instance.
             *
             * We don't include a secondary alias if there's already a primary alias covering it.
             */
            for (String target : targets) {
                String secondaryAlias = "http://pandora.nla.gov.au/pan/" + piAndDate + "/" + Urls.removeScheme(target);
                if (!aliases.contains(secondaryAlias) && isSaneUrl(secondaryAlias)) {
                    writeAlias(secondaryAlias, target);
                }
            }
        }

        private void writeAlias(String alias, String target) {
            out.print("@alias ");
            out.print(alias);
            out.print(" ");
            out.println(target);
        }

        public static boolean isSaneUrl(String url) {
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

        /**
         * Strips the pi and instance date from a PANDORA path if present.
         */
        static String cleanHttrackPath(String path, String piAndDate) {
            path = StringUtils.removeStart(path, "/");
            String piAndLegacyDate = StringUtils.removeEnd(piAndDate, "-0000");
            if (path.startsWith(piAndDate + "/")) {
                return path.substring(piAndDate.length() + 1); // 1234/20010101-1234/(example.org/index.html)
            } else if (path.startsWith(piAndLegacyDate + "/")) {
                return path.substring(piAndLegacyDate.length() + 1); // 1234/20010101/(example.org/index.html)
            } else {
                return path; // (example.org/index.html)
            }
        }
    }

    public static String encodeJsonRequest(InputStream stream) throws IOException {
        StringBuilder output = new StringBuilder();
        JsonParser parser = new JsonFactory().createParser(stream);
        Map<String,Long> serials = new HashMap<>();
        Deque<String> nameStack = new ArrayDeque<>();
        String name = null;
        while (parser.nextToken() != null && output.length() < 4096) {
            switch (parser.currentToken()) {
                case FIELD_NAME:
                    name = parser.getCurrentName();
                    break;
                case VALUE_FALSE:
                case VALUE_TRUE:
                case VALUE_NUMBER_FLOAT:
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NULL:
                    if (name != null) {
                        long serial = serials.compute(name, (key, value) -> value == null ? 1 : value + 1);
                        String key = name;
                        if (serial > 1) {
                            key += "." + serial + "_";
                        }
                        output.append('&');
                        output.append(URIUtil.encodeWithinQuery(key));
                        output.append('=');
                        String value;
                        switch (parser.currentToken()) {
                            case VALUE_NULL:
                                value = "None"; // using Python names for pywb compatibility
                                break;
                            case VALUE_FALSE:
                                value = "False";
                                break;
                            case VALUE_TRUE:
                                value = "True";
                                break;
                            case VALUE_NUMBER_INT:
                                value = String.valueOf(parser.getLongValue());
                                break;
                            case VALUE_NUMBER_FLOAT:
                                value = String.valueOf(parser.getDoubleValue());
                                break;
                            default:
                                value = URIUtil.encodeWithinQuery(parser.getValueAsString());
                        }
                        output.append(value);
                    }
                    break;
                case START_OBJECT:
                    if (name != null) {
                        nameStack.push(name);
                    }
                    break;
                case END_OBJECT:
                    name = nameStack.isEmpty() ? null : nameStack.pop();
                    break;
                case START_ARRAY:
                case END_ARRAY:
                    break;
                default:
                    throw new IllegalStateException("Unexpected: " + parser.currentToken());
            }
        }
        return output.toString();
    }
}
