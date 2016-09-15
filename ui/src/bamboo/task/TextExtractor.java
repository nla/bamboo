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
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.regex.Pattern;

public class TextExtractor {
    static final int pdfDiskOffloadThreshold = 32 * 1024 * 1024;
    static final int maxDocSize = 0x100000;
    static final long timeLimitMillis = 5000;

    private boolean boilingEnabled = false;
    private boolean usePdfBox = false;

    public Document extract(ArchiveRecord record) throws TextExtractionException {
        Document doc = new Document();
        ArchiveRecordHeader warcHeader = record.getHeader();

        String url = WarcUtils.getCleanUrl(warcHeader);

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
        doc.setUrl(url);
        doc.setContentLength(warcHeader.getContentLength());
        Instant instant = LocalDateTime.parse(arcDate, WarcUtils.arcDateFormat).atOffset(ZoneOffset.UTC).toInstant();
        doc.setDate(Date.from(instant));
        doc.setWarcOffset(warcHeader.getOffset());

        try {
            doc.setSite(topPrivateDomain(url));
        } catch (MalformedURLException e) {
            throw new TextExtractionException(e);
        }

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
                    extractHtml(record, doc);
                    break;
                case "application/pdf":
                    if (usePdfBox) {
                        extractPdfBox(record, doc);
                    } else {
                        extractPdf(record, doc);
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
            CharBuffer buf = CharBuffer.allocate(maxDocSize);
            PdfTextExtractor extractor = new PdfTextExtractor(pdfReader);
            try {
                for (int i = 1; i <= pdfReader.getNumberOfPages() && System.currentTimeMillis() < deadline; ++i) {
                    String text = extractor.getTextFromPage(i);
                    buf.append(text.replace("\uFFFF", ""));
                    buf.append(' ');
                }
            } catch (BufferOverflowException e) {
                // reached maxDocSize amount of content
            }
            buf.flip();
            doc.setText(buf.toString());

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
        StringWriter sw = new StringWriter();
        try (PDDocument pdf = PDDocument.load(stream, MemoryUsageSetting.setupTempFileOnly())) {
            String title = pdf.getDocumentInformation().getTitle();
            if (title != null) {
                doc.setTitle(title);
            }

            PDFTextStripper stripper = new PDFTextStripper();
            stripper.writeText(pdf, new TruncatingWriter(sw, maxDocSize, System.currentTimeMillis() + timeLimitMillis));
        } catch (BufferOverflowException e) {
            // reached maxDocSize, just stop early
        } catch (Exception e) {
            throw new TextExtractionException(e);
        }
        doc.setText(sw.toString());
    }

    public void setUsePdfBox(boolean usePdfBox) {
        this.usePdfBox = usePdfBox;
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
            if (remaining() > 0 && System.currentTimeMillis() < deadline) {
                super.write(i);
                written++;
            } else {
                throw new BufferOverflowException();
            }
        }

        @Override
        public void write(String s, int off, int len) throws IOException {
            if (remaining() <= 0 || System.currentTimeMillis() >= deadline) {
                throw new BufferOverflowException();
            }
            int clamped = (int)Math.min(len, remaining());
            super.write(s, off, clamped);
            written += clamped;
        }

        @Override
        public void write(char[] chars, int off, int len) throws IOException {
            if (remaining() <= 0 || System.currentTimeMillis() >= deadline) {
                throw new BufferOverflowException();
            }
            int clamped = (int)Math.min(len, remaining());
            super.write(chars, off, clamped);
            written += clamped;
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
