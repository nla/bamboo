package bamboo.task;

import com.google.common.net.InternetDomainName;
import com.lowagie.text.pdf.PdfReader;
import com.lowagie.text.pdf.parser.PdfTextExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.netpreserve.urlcanon.Canonicalizer;
import org.netpreserve.urlcanon.ParsedUrl;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextExtractor {
    static final int pdfDiskOffloadThreshold = 32 * 1024 * 1024;
    static final int maxDocSize = 0x100000;
    static final long timeLimitMillis = 5000;

    private boolean boilingEnabled = false;
    private boolean usePdfBox = false;
    private boolean useTika = false;

    public static final Pattern PANDORA_REGEX = Pattern.compile("http://pandora.nla.gov.au/pan/[0-9]+/[0-9-]+/([^/.]+\\.[^/]+/.*)");
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

        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            uri = null;
        }

        if (WarcUtils.isResponseRecord(warcHeader)) {
            HttpHeader httpHeader;
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

        String digest = (String) warcHeader.getHeaderValue("WARC-Payload-Digest");
        if (digest != null) {
            if (digest.startsWith("sha1:")) {
                digest = digest.substring(5);
            }
            doc.setContentSha1(digest);
        }

        if (doc.getContentType() == null) {
            throw new TextExtractionException("no content type");
        }

        try {
            switch (doc.getContentType()) {
                case "text/html":
                    if (useTika) {
                        extractTika(record, doc, uri);
                    } else {
                        extractHtml(record, doc);
                    }
                    break;
                case "application/pdf":
                    if (usePdfBox) {
                        extractPdfBox(record, doc);
                    } else {
                        extractPdf(record, doc);
                    }
                    break;
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
                    if (useTika) {
                        extractTika(record, doc, uri);
                    } else {
                        doc.setTextError("not implemented for content-type");
                    }
                    break;
                default:
                    doc.setTextError("not implemented for content-type");
                    break;
            }
        } catch (TextExtractionException e) {
            doc.setTextError(e.getMessage());
        }

        return doc;
    }

    public static void extractTika(InputStream record, Document doc, URI baseUrl) throws TextExtractionException {
        Tika tika = new Tika();
        Metadata metadata = new Metadata();
        try {
            ParseContext parseContext = new ParseContext();
            LinkContentHandler linkHandler = new LinkContentHandler();
            BodyContentHandler bodyHandler = new BodyContentHandler(maxDocSize);
            TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, bodyHandler);
            tika.getParser().parse(record, teeHandler, metadata, parseContext);
            String text = bodyHandler.toString();
            doc.setText(text);
            doc.setTitle(metadata.get(TikaCoreProperties.TITLE));
            doc.setDescription(getAny(metadata, "description", "DC.description", "DC.Description", "dcterms.description"));
            doc.setKeywords(getAny(metadata, "keywords", "DC.keywords", "DC.Keywords", "dcterms.keywords"));
            doc.setPublisher(getAny(metadata, "publisher", "DC.publisher", "DC.Publisher", "dcterms.publisher"));
            doc.setCreator(getAny(metadata, "creator", "DC.creator", "DC.Creator", "dcterms.creator"));
            doc.setContributor(getAny(metadata, "contributor", "DC.contributor", "DC.Contributor", "dcterms.contributor"));
            doc.setCoverage(getAny(metadata, "coverage", "DC.coverage", "DC.Coverage", "dcterms.coverage"));

            List<LinkInfo> links = new ArrayList<>();
            for (Link link: linkHandler.getLinks()) {
                if ("".equals(link.getUri())) {
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
                    String url = baseUrl.resolve(link.getUri()).toString();
                    info.setUrl(WarcUtils.cleanUrl(url));
                }
                links.add(info);
            }
            doc.setLinks(links);
        } catch (IOException | TikaException | SAXException e) {
            throw new TextExtractionException("Tika failed", e);
        }
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

    private void extractHtml(ArchiveRecord record, Document doc) throws TextExtractionException {
        try {
            BoundedInputStream in = new BoundedInputStream(record, maxDocSize);
            TextDocument textDoc = new BoilerpipeSAXInput(new InputSource(in)).getTextDocument();
            doc.setTitle(textDoc.getTitle());
            doc.setText(textDoc.getText(true, true).replace("\uFFFF", ""));
            if (boilingEnabled) {
                DefaultExtractor.INSTANCE.process(textDoc);
                doc.setBoiled(textDoc.getContent().replace("\uFFFF", ""));
            }
        } catch (SAXException | BoilerpipeProcessingException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
            throw new TextExtractionException(e);
        }
    }

    private static void extractPdf(ArchiveRecord record, Document doc) throws TextExtractionException {
        doc.setTitle(record.getHeader().getUrl());

        try {
            if (record.getHeader().getLength() > pdfDiskOffloadThreshold) {
                // PDFReader needs (uncompressed) random access to the file.  When given a stream it loads the whole
                // lot into a memory buffer. So for large records let's decompress to a temporary file first.
                Path tmp = Files.createTempFile("bamboo-solr-tmp", ".pdf");
                Files.copy(record, tmp, StandardCopyOption.REPLACE_EXISTING);
                try {
                    extractPdfContent(new PdfReader(tmp.toString()), doc);
                } finally {
                    try {
                        Files.deleteIfExists(tmp);
                    } catch (IOException e) {
                        // we can't do anything
                    }
                }
            } else {
                extractPdfContent(new PdfReader(record), doc);
            }
        } catch (NoClassDefFoundError | RuntimeException | IOException e) {
            throw new TextExtractionException(e);
        }
    }

    static void extractPdfContent(PdfReader pdfReader, Document doc) throws TextExtractionException, IOException {
        try {
            long deadline = System.currentTimeMillis() + timeLimitMillis;
            StringWriter sw = new StringWriter();
            TruncatingWriter tw = new TruncatingWriter(sw, maxDocSize, deadline);
            PdfTextExtractor extractor = new PdfTextExtractor(pdfReader);
            try {
                for (int i = 1; i <= pdfReader.getNumberOfPages(); ++i) {
                    String text = extractor.getTextFromPage(i);
                    tw.append(text.replace("\uFFFF", ""));
                    tw.append(' ');
                }
                doc.setText(sw.toString());
            } catch (TruncatedException e) {
                // reached limits, stop early and save what we got
                doc.setText(sw.toString());
                doc.setTextError("truncatedPdf " + e.getMessage());
            }

            // extract the title from the metadata if it has one
            Object metadataTitle = pdfReader.getInfo().get("Title");
            if (metadataTitle != null && metadataTitle instanceof String) {
                doc.setTitle((String) metadataTitle);
            }
        } catch (Error e) {
            if (e.getClass() == Error.class && (e.getMessage() == null || e.getMessage().startsWith("Unable to process ToUnicode map"))) {
                // pdf reader abuses java.lang.Error sometimes to indicate a parse error
                throw new TextExtractionException(e);
            } else {
                // let everything else (eg OutOfMemoryError) through
                throw e;
            }
        }
    }

    static void extractPdfBox(InputStream stream, Document doc) throws TextExtractionException {
        long deadline = System.currentTimeMillis() + timeLimitMillis;
        StringWriter sw = new StringWriter();
        try (PDDocument pdf = PDDocument.load(stream, MemoryUsageSetting.setupTempFileOnly())) {
            String title = pdf.getDocumentInformation().getTitle();
            if (title != null) {
                doc.setTitle(title);
            }

            doc.setKeywords(pdf.getDocumentInformation().getKeywords());
            doc.setPublisher(pdf.getDocumentInformation().getAuthor());
            doc.setCoverage(pdf.getDocumentInformation().getSubject());

            PDFTextStripper stripper = new PDFTextStripper();
            stripper.writeText(pdf, new TruncatingWriter(sw, maxDocSize, deadline));
            doc.setText(sw.toString());
        } catch (TruncatedException e) {
            // reached limits, write what we got and then stop early
            doc.setText(sw.toString());
            doc.setTextError("truncatedPdf " + e.getMessage());
        } catch (Exception e) {
            throw new TextExtractionException(e);
        }
    }

    public void setUsePdfBox(boolean usePdfBox) {
        this.usePdfBox = usePdfBox;
    }

    static class TruncatedException extends RuntimeException {
        public TruncatedException(String message) {
            super(message);
        }
    }

    public void setUseTika(boolean useTika) {
        this.useTika = useTika;
    }

    static class TruncatingWriter extends FilterWriter {
        private final long limit;
        private final long deadline;
        private long written;

        TruncatingWriter(Writer out, long limit, long deadline) {
            super(out);
            this.limit = limit;
            this.deadline = deadline;
        }

        @Override
        public void write(int i) throws IOException {
            checkLimits();
            super.write(i);
            written++;
        }

        @Override
        public void write(String s, int off, int len) throws IOException {
            checkLimits();
            int clamped = (int)Math.min(len, remaining());
            super.write(s, off, clamped);
            written += clamped;
        }

        @Override
        public void write(char[] chars, int off, int len) throws IOException {
            checkLimits();
            int clamped = (int)Math.min(len, remaining());
            super.write(chars, off, clamped);
            written += clamped;
        }

        private void checkLimits() {
            if (remaining() <= 0) {
                throw new TruncatedException("space");
            } else if (System.currentTimeMillis() >= deadline) {
                throw new TruncatedException("time");
            }
        }

        private long remaining() {
            return limit - written;
        }
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

    /**
     * Sets whether to use boilerpipe's article extractor to try to filter out the main article from a page. The article
     * text will be stored in Document.boiled.
     */
    public void setBoilingEnabled(boolean boilingEnabled) {
        this.boilingEnabled = boilingEnabled;
    }
}
