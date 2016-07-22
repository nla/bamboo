package bamboo.task;

import com.google.common.net.InternetDomainName;
import com.lowagie.text.pdf.PdfReader;
import com.lowagie.text.pdf.parser.PdfTextExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.apache.commons.io.input.BoundedInputStream;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
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

        switch (doc.getContentType()) {
            case "text/html":
                extractHtmlContent(record, doc);
                return doc;
            case "application/pdf":
                extractPdfContent(record, doc);
                return doc;
            default:
                throw new TextExtractionException("unhandled content type: " + doc.getContentType());
        }
    }

    private static void extractHtmlContent(ArchiveRecord record, Document doc) throws TextExtractionException {
        try {
            BoundedInputStream in = new BoundedInputStream(record, maxDocSize);
            TextDocument textDoc = new BoilerpipeSAXInput(new InputSource(in)).getTextDocument();
            doc.setTitle(textDoc.getTitle());
            doc.setText(textDoc.getText(true, true).replace("\uFFFF", ""));
            doc.setBoiled(textDoc.getContent().replace("\uFFFF", ""));
            DefaultExtractor.INSTANCE.process(textDoc);
        } catch (SAXException | BoilerpipeProcessingException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
            throw new TextExtractionException(e);
        }
    }

    private static void extractPdfContent(ArchiveRecord record, Document doc) throws TextExtractionException {
        Path tmp = null;
        try {
            CharBuffer buf = CharBuffer.allocate(maxDocSize);
            PdfReader pdfReader;

            if (record.getHeader().getLength() > pdfDiskOffloadThreshold) {
                // PDFReader needs (uncompressed) random access to the file.  When given a stream it loads the whole
                // lot into a memory buffer. So for large records let's decompress to a temporary file first.
                tmp = Files.createTempFile("bamboo-solr-tmp", ".pdf");
                Files.copy(record, tmp, StandardCopyOption.REPLACE_EXISTING);
                pdfReader = new PdfReader(tmp.toString());
            } else {
                pdfReader = new PdfReader(record);
            }

            PdfTextExtractor extractor = new PdfTextExtractor(pdfReader);
            try {
                for (int i = 1; i <= pdfReader.getNumberOfPages(); ++i) {
                    String text = extractor.getTextFromPage(i);
                    buf.append(text.replace("\uFFFF", ""));
                    buf.append(' ');
                }
            } catch (BufferOverflowException e) {
                // reached maxDocSize amount of content
            }
            buf.flip();
            doc.setTitle(record.getHeader().getUrl());
            doc.setText(buf.toString());
        } catch (NoClassDefFoundError | RuntimeException | IOException e) {
            throw new TextExtractionException(e);
        } catch (Error e) {
            if (e.getMessage() == null || e.getMessage().startsWith("Unable to process ToUnicode map")) {
                // pdf reader abuses java.lang.Error sometimes to indicate a parse error
                throw new TextExtractionException(e);
            } else {
                // let everything else (eg OutOfMemoryError) through
                throw e;
            }
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException e) {
                    // we can't do anything
                }
            }
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

}
