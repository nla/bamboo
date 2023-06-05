package bamboo.task;

import com.google.common.net.InternetDomainName;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.fork.ForkParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.Base32;
import org.brotli.dec.BrotliInputStream;
import org.netpreserve.urlcanon.Canonicalizer;
import org.netpreserve.urlcanon.ParsedUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TextExtractor implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(TextExtractor.class);
    static final int maxDocSize = 0x100000;

    public static final Pattern PANDORA_REGEX = Pattern.compile("http://pandora.nla.gov.au/pan/[0-9]+/[0-9-]+/([^/.]+\\.[^/]+/.*)");
    private final Parser parser;
    private final Path logbackConfig;

    public TextExtractor() {
        try {
            logbackConfig = Files.createTempFile("bamboo-tika-logback", ".xml");
            try (InputStream stream = getClass().getResourceAsStream("/tika-logback.xml")) {
                Files.copy(stream, logbackConfig, REPLACE_EXISTING);
            }

            TikaConfig config = new TikaConfig(getClass().getResource("tika.xml"));
            ForkParser parser = new ForkParser(getClass().getClassLoader(), new AutoDetectParser(config));
            parser.setServerParseTimeoutMillis(15000); // don't spend too long on any one record
            Path javaBinary = Path.of(System.getProperty("java.home"), "bin", "java");
            parser.setJavaCommand(Arrays.asList(javaBinary.toString(), "-Xmx512m", "-Dlogback.configurationFile=" + logbackConfig));
            if (System.getenv("TIKA_POOL_SIZE") != null) {
                parser.setPoolSize(Integer.parseInt(System.getenv("TIKA_POOL_SIZE")));
            }
            this.parser = parser;
        } catch (Exception e) {
            close();
            throw new RuntimeException("Error configuring tika via tika.xml", e);
        }
    }

    public static void setUrls(Document doc, String url) throws TextExtractionException {
        String deliveryUrl = url;
        Matcher m = PANDORA_REGEX.matcher(url);
        if (m.matches()) {
            // TODO: consult url.map
            String hackedOffUrl = "http://" + m.group(1);
            url = hackedOffUrl;
        }
        doc.setUrl(url);
        ParsedUrl parse = ParsedUrl.parseUrl(deliveryUrl);
        Canonicalizer.AGGRESSIVE.canonicalize(parse);
        doc.setDeliveryUrl(parse.toString());

        try {
            doc.setHost(new URL(url).getHost());
            doc.setSite(topPrivateDomain(url));
        } catch (MalformedURLException e) {
            throw new TextExtractionException(e);
        }
    }

    public Document extract(ArchiveRecord record) throws TextExtractionException {
        Document doc = new Document();
        ArchiveRecordHeader warcHeader = record.getHeader();

        String url = WarcUtils.getCleanUrl(warcHeader);

        HttpHeader httpHeader = null;
        if (WarcUtils.isResponseRecord(warcHeader)) {
            try {
                httpHeader = HttpHeader.parse(record, url);
            } catch (IOException e) {
                throw new TextExtractionException("parsing http header: " + e.getMessage(), e);
            }
            if (httpHeader == null) {
                throw new TextExtractionException("response record missing http header");
            }
            doc.setContentType(HttpHeader.cleanContentType(httpHeader.contentType));
            doc.setStatusCode(httpHeader.status);
            if (httpHeader.location != null) {
                LinkInfo link = new LinkInfo();
                link.setType("location");
                link.setUrl(httpHeader.location);
                link.setHref(httpHeader.rawLocation);
                doc.addLink(link);
            }
        } else if (WarcUtils.isResourceRecord(warcHeader)) {
            doc.setContentType(HttpHeader.cleanContentType((String) warcHeader.getHeaderValue("Content-Type")));
            doc.setStatusCode(200);
        } else {
            throw new TextExtractionException("unhandled WARC record type");
        }

        String arcDate = WarcUtils.getArcDate(warcHeader);
        setUrls(doc, url);
        doc.setContentLength(warcHeader.getContentLength());
        Instant instant = LocalDateTime.parse(arcDate, WarcUtils.arcDateFormat).atOffset(ZoneOffset.UTC).toInstant();
        doc.setDate(Date.from(instant));
        doc.setWarcOffset(warcHeader.getOffset());

        InputStream contentStream = record;
        String digest = (String) warcHeader.getHeaderValue("WARC-Payload-Digest");
        if (digest != null) {
            if (digest.startsWith("sha1:")) {
                digest = digest.substring(5);
            }
            doc.setContentSha1(digest);
        } else {
            // no digest was available so let's calculate one on the fly
            try {
                contentStream = new DigestInputStream(record, MessageDigest.getInstance("SHA1"));
            } catch (NoSuchAlgorithmException e) {
                log.warn("SHA1 not available", e);
            }
        }

        if (doc.getContentType() == null) {
            throw new TextExtractionException("no content type");
        }

        URI uri;
        try {
            uri = new URI(doc.getUrl());
        } catch (URISyntaxException e) {
            uri = null;
        }

        try {
            if (httpHeader != null && httpHeader.brotli) {
                contentStream = new BrotliInputStream(contentStream);
            }
            switch (doc.getContentType()) {
                case "text/html":
                case "application/pdf":
                case "application/vnd.ms-excel":
                case "text/csv":
                case "application/csv":
                case "application/vnd.ms-powerpoint":
                case "application/msword":
                case "application/vnd.ms-word.document.macroEnabled.12":
                case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                case "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                case "application/vnd.oasis.opendocument.presentation":
                case "application/vnd.oasis.opendocument.text":
                case "application/vnd.oasis.opendocument.spreadsheet":
                    extractTika(contentStream, doc, uri);
                    break;
                default:
                    doc.setTextError("not implemented for content-type");
                    break;
            }
        } catch (TextExtractionException | IOException e) {
            doc.setTextError(e.getMessage());
        }

        if (contentStream instanceof DigestInputStream) {
            fullyConsume(contentStream);
            byte[] digestBytes = ((DigestInputStream)contentStream).getMessageDigest().digest();
            doc.setContentSha1(Base32.encode(digestBytes));
        }

        return doc;
    }

    private void fullyConsume(InputStream stream) {
        byte[] buf = new byte[8192];
        try {
            while (true) {
                if (stream.read(buf) == -1) break;
            }
        } catch (IOException e) {
            log.warn("error consuming rest of stream", e);
        }
    }

    public void extractTika(InputStream record, Document doc, URI baseUrl) throws TextExtractionException {
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, doc.getContentType());
        try {
            ParseContext parseContext = new ParseContext();
            LinkContentHandler linkHandler = new LinkContentHandler(true);
            BodyContentHandler bodyHandler = new BodyContentHandler(maxDocSize);
            HeadingContentHandler headingHandler = new HeadingContentHandler();
            TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, bodyHandler, headingHandler);

            parser.parse(record, teeHandler, metadata, parseContext);

            doc.setText(clean(bodyHandler.toString()));
            doc.setTitle(clean(getAny(metadata, TikaCoreProperties.TITLE.getName())));
            doc.setDescription(clean(getAny(metadata, "description", "DC.description", "DC.Description", "dcterms.description")));
            doc.setKeywords(clean(getAny(metadata, "Keywords", "keywords", "DC.keywords", "DC.Keywords", "dcterms.keywords")));
            doc.setPublisher(clean(getAny(metadata, "publisher", "DC.publisher", "DC.Publisher", "dcterms.publisher")));
            doc.setCreator(clean(getAny(metadata, "creator", "DC.creator", "DC.Creator", "dcterms.creator")));
            doc.setContributor(clean(getAny(metadata, "contributor", "DC.contributor", "DC.Contributor", "dcterms.contributor")));
            doc.setCoverage(clean(getAny(metadata, "coverage", "DC.coverage", "DC.Coverage", "dcterms.coverage", "subject", "Subject")));
            doc.setH1(headingHandler.getHeadings());
            doc.setOgSiteName(clean(metadata.get("og:site_name")));
            doc.setOgTitle(clean(metadata.get("og:title")));

            for (Link link: linkHandler.getLinks()) {
                if ("".equals(link.getUri()) ||
                        startsWithIgnoreCase(link.getUri(), "data:")) {
                    continue;
                }
                LinkInfo info = new LinkInfo();
                info.setType(link.getType());
                info.setHref(link.getUri());
                if (!"".equals(link.getText())) {
                    info.setText(link.getText());
                }
                if (!"".equals(link.getRel())) {
                    info.setRel(link.getRel());
                }
                if (!"".equals(link.getTitle())) {
                    info.setTitle(link.getTitle());
                }
                if (baseUrl != null) {
                    String url = null;
                    try {
                        url = baseUrl.resolve(link.getUri()).toString();
                        info.setUrl(WarcUtils.cleanUrl(url));
                    } catch (IllegalArgumentException e) {
                        // bad url
                    } catch (StackOverflowError e) {
                        log.warn("URL caused stackoverflow: " + url);
                    }
                }
                doc.addLink(info);
            }
        } catch (IOException | TikaException | SAXException e) {
            throw new TextExtractionException("Tika failed", e);
        }
    }

    public static final Pattern MULTISPACE = Pattern.compile("\\s{2,}");
    private String clean(String s) {
        if (s == null) return null;
        return MULTISPACE.matcher(s.trim()).replaceAll(" ");
    }

    private static boolean startsWithIgnoreCase(String str, String prefix) {
        return str.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    public static String getAny(Metadata metadata, String... keys) {
        for (String key : keys) {
            String value = metadata.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private final static Pattern WWW_PREFIX = Pattern.compile("^www[0-9]*\\.");

    private static String topPrivateDomain(String url) throws MalformedURLException {
        String host = new URL(url).getHost();
        try {
            InternetDomainName domain = InternetDomainName.from(host);
            return domain.topPrivateDomain().toString();
        } catch (IllegalStateException | IllegalArgumentException e) {
            // IP addresses, hosts which don't have a known TLD etc
            return WWW_PREFIX.matcher(host).replaceFirst("");
        }
    }

    public static void extract(ArchiveReader reader, OutputStream out) throws IOException {
        try (TextExtractor extractor = new TextExtractor()) {
            extractor.extractAll(reader, out);
        }
    }

    void extractAll(ArchiveReader reader, OutputStream out) throws IOException {
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
                .setPrettyPrinting().create();
        try (JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(out))) {
            writer.beginArray();
            for (ArchiveRecord record : reader) {
                String url = record.getHeader().getUrl();
                if (url == null) continue;
                try {
                    Document doc = extract(record);
                    gson.toJson(doc, Document.class, writer);
                } catch (TextExtractionException e) {
                    continue; // skip it
                }
            }
            writer.endArray();
            writer.flush();
        }
    }

    public static void main(String[] args) throws IOException {
        try (ArchiveReader reader = ArchiveReaderFactory.get(args[0])) {
            extract(reader, System.out);
        }
    }

    @Override
    public void close(){
        if (logbackConfig != null) {
            try {
                Files.deleteIfExists(logbackConfig);
            } catch (IOException e) {
                log.warn("Unable to cleanup " + logbackConfig, e);
            }
        }
    }
}
