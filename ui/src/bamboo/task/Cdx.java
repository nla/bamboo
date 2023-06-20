package bamboo.task;

import bamboo.crawl.RecordStats;
import bamboo.util.Urls;
import org.apache.commons.lang.StringUtils;
import org.netpreserve.jwarc.*;
import org.netpreserve.jwarc.cdx.CdxRequestEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bamboo.task.WarcUtils.cleanUrl;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.ZoneOffset.UTC;

public class Cdx {
    final static Pattern PANDORA_URL_MAP = Pattern.compile("^http://pandora\\.nla\\.gov\\.au/pan/([0-9]+/[0-9-]+)/url\\.map$");
    final static Pattern PANDORA_RECURSIVE_URL = Pattern.compile("^http://pandora\\.nla\\.gov\\.au/pan/([0-9]+/[0-9-]+)/pandora\\.nla\\.gov\\.au/pan/.*");
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

                    String redirect = null;
                    if (record instanceof WarcResponse response && status >= 300 && status <= 399) {
                        redirect = response.http().headers().first("Location").orElse(null);
                    }
                    if (redirect != null) {
                        try {
                            redirect = new URI(url).resolve(redirect).toASCIIString();
                        } catch (URISyntaxException | IllegalArgumentException e) {
                            log.debug("Couldn't resolve Location header '{}' against URL '{}'", redirect, url, e);
                            redirect = null;
                        }
                    }

                    if (instant.isBefore(YEAR1990)) {
                        // garbage. skip.
                        record = reader.next().orElse(null);
                        continue;
                    }

                    if (digest == null) {
                        digest = WarcUtils.calcDigest(payload.body().stream());
                    }

//                    long contentLength = payload.body().size();
//                    if (contentLength == -1) {
//                        try {
//                            payload.body().consume();
//                        } catch (EOFException e) {
//                            // record seems to be truncated. that's ok.
//                            log.warn(e + " for record " + id + " in " + filename + " at " + position);
//                        }
//                        contentLength = payload.body().position();
//                    }

                    // advance to the next record so we can calculate the length
                    record = reader.next().orElse(null);
                    long length = reader.position() - position;

                    String urlKey = null;

                    // check for a corresponding request record
                    while (record instanceof WarcCaptureRecord && ((WarcCaptureRecord) record).concurrentTo().contains(id)) {
                        if (record instanceof WarcRequest) {
                            HttpRequest httpRequest = ((WarcRequest) record).http();
                            if (httpRequest.method().equals("POST") || httpRequest.method().equals("PUT")) {
                                String encodedRequest = CdxRequestEncoder.encode(httpRequest);
                                if (encodedRequest != null) {
                                    String rawUrlKey = url +
                                            (url.contains("?") ? '&' : '?')
                                            + encodedRequest;
                                    urlKey = URIs.toNormalizedSurt(rawUrlKey);
                                }
                                break;
                            }
                        }

                        record = reader.next().orElse(null);
                    }

                    if (urlKey == null) urlKey = URIs.toNormalizedSurt(url);

                    out.printf("%s %s %s %s %d %s %s - %d %d %s%n", urlKey, date, url, type, status, digest,
                            redirect == null ? "-" : redirect, length, position, filename);
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
}
