package bamboo.task;

import bamboo.core.Config;
import bamboo.core.Db;
import bamboo.core.DbPool;
import com.google.common.net.InternetDomainName;
import com.itextpdf.text.exceptions.InvalidPdfException;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfReaderContentParser;
import com.itextpdf.text.pdf.parser.SimpleTextExtractionStrategy;
import com.itextpdf.text.pdf.parser.TextExtractionStrategy;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;
import java.time.*;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SolrIndexer {

    static int MAX_DOC_SIZE = 0x1000000;
    static int COMMIT_WITHIN_MS = 10000;

    private final Config config;
    private final DbPool dbPool;
    private SolrServer solr;

    public SolrIndexer(Config config, DbPool dbPool) {
        this.config = config;
        this.dbPool = dbPool;
        String solrUrl = System.getenv("BAMBOO_SOLR_URL");
        String solrZoo = System.getenv("BAMBOO_SOLR_ZOO");
        if (solrUrl != null) {
            solr = new HttpSolrServer(solrUrl);
        } else if (solrZoo != null) {
            solr = new CloudSolrServer(solrZoo);
        } else {
            throw new IllegalStateException("BAMBOO_SOLR_URL or BAMBOO_SOLR_ZOO env var must be set");
        }
    }

    public void run() {
        ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try (Db db = dbPool.take()) {
            for (Db.Warc warc : db.findWarcsToSolrIndex()) {
                threadPool.submit(() -> indexWarc(warc));
            }
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void indexWarc(Db.Warc warc) {
        System.out.println("Solr indexing " + warc.id + " " + warc.path);
        try (ArchiveReader reader = ArchiveReaderFactory.get(warc.path.toFile())) {
            for (ArchiveRecord record : reader) {
                SolrInputDocument doc = makeDoc(record);
                if (doc != null) {
                    solr.add(doc, COMMIT_WITHIN_MS);
                }
            }
            try (Db db = dbPool.take()) {
                db.setWarcSolrIndexed(warc.id, System.currentTimeMillis());
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        } catch (SolrServerException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        System.out.println("Finished Solr indexing " + warc.id + " " + warc.path);
    }

    public static SolrInputDocument makeDoc(ArchiveRecord record) throws IOException {
        ArchiveRecordHeader warcHeader = record.getHeader();
        if (!Warcs.isResponseRecord(warcHeader)) return null;

        String url = Warcs.getCleanUrl(warcHeader);
        HttpHeader httpHeader = HttpHeader.parse(record, url);
        if (httpHeader == null || httpHeader.status < 200 || httpHeader.status > 299) {
            return null;
        }

        String contentType = httpHeader.getCleanContentType();
        String arcDate = Warcs.getArcDate(warcHeader);

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", url + " " + arcDate);
        doc.addField("url", url);
        doc.addField("length", warcHeader.getContentLength());
        doc.addField("code", httpHeader.status);
        Instant instant = LocalDateTime.parse(arcDate, Warcs.arcDateFormat).atOffset(ZoneOffset.UTC).toInstant();
        doc.addField("date", Date.from(instant));
        InternetDomainName domain = InternetDomainName.from(new URL(url).getHost());
        doc.addField("site", domain.topPrivateDomain().toString());
        doc.addField("type", contentType);

        String digest = (String) warcHeader.getHeaderValue("WARC-Payload-Digest");
        if (digest != null) {
            if (digest.startsWith("sha1:")) {
                digest = digest.substring(5);
            }
            doc.addField("sha1", digest);
        }

        if (contentType.equals("text/html")) {
            return extractHtmlContent(record, doc);
        } else if (contentType.equals("application/pdf")) {
            return extractPdfContent(record, doc);
        } else {
            return null; // unknown type
        }
    }

    private static SolrInputDocument extractHtmlContent(ArchiveRecord record, SolrInputDocument doc) {
        try {
            BoundedInputStream in = new BoundedInputStream(record, MAX_DOC_SIZE);
            TextDocument textDoc = new BoilerpipeSAXInput(new InputSource(in)).getTextDocument();
            doc.addField("title", textDoc.getTitle());
            DefaultExtractor.INSTANCE.process(textDoc);
            doc.addField("content", textDoc.getText(true, true));
            doc.addField("boiled", textDoc.getContent());
            return doc;
        } catch (SAXException | BoilerpipeProcessingException e) {
            return null;
        }
    }

    private static SolrInputDocument extractPdfContent(ArchiveRecord record, SolrInputDocument doc) throws IOException {
        try {
            CharBuffer buf = CharBuffer.allocate(MAX_DOC_SIZE);
            PdfReader pdfReader = new PdfReader(record);
            PdfReaderContentParser pdfParser = new PdfReaderContentParser(pdfReader);
            try {
                for (int i = 1; i <= pdfReader.getNumberOfPages(); ++i) {
                    TextExtractionStrategy strategy = pdfParser.processContent(i,
                            new SimpleTextExtractionStrategy());
                    buf.append((CharSequence) strategy.getResultantText());
                    buf.append(' ');
                }
            } catch (BufferOverflowException e) {
                // reached MAX_DOC_SIZE amount of content
            }
            doc.addField("title", record.getHeader().getUrl());
            doc.addField("content", buf.toString());
            return doc;
        } catch (InvalidPdfException | NoClassDefFoundError e) {
            return null; // invalid or encrypted pdf
        }
    }

    public static void main(String args[]) {
        for (int i = 0; i < args.length; i++) {
            try (ArchiveReader reader = ArchiveReaderFactory.get(args[i])) {
                for (ArchiveRecord record : reader) {
                    SolrInputDocument doc = makeDoc(record);
                    if (doc != null) {
                        System.out.println(doc.toString());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
