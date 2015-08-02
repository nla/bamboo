package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import bamboo.util.SurtFilter;
import com.google.common.net.InternetDomainName;
import com.lowagie.text.exceptions.InvalidPdfException;
import com.lowagie.text.pdf.PdfReader;
import com.lowagie.text.pdf.parser.PdfTextExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.url.SURT;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class SolrIndexer implements Runnable {

    static final int PDF_DISK_OFFLOAD_THRESHOLD = 32 * 1024 * 1024;
    static final int MAX_DOC_SIZE = 0x100000;
    static final int COMMIT_WITHIN_MS = 300000;

    private final DbPool dbPool;
    private SolrServer solr;

    public SolrIndexer(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public void run() {
        while (true) {
            List<Db.Warc> warcs;

            try (Db db = dbPool.take()) {
                warcs = db.findWarcsToSolrIndex();
            }

            if (warcs.isEmpty()) {
                break;
            }

            indexWarcs(warcs);
        }
    }

    public void indexWarcs(List<Db.Warc> warcs) {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        try {
            for (Db.Warc warc : warcs) {
                threadPool.submit(() -> indexWarc(warc));
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdownNow();
        }
    }

    static class Solr {
        private final SurtFilter filter;
        private final SolrServer server;
        private final Db.Collection collection;

        public Solr(Db.CollectionWithFilters collection) {
            this.collection = collection;
            server = new HttpSolrServer(collection.solrUrl);
            filter = new SurtFilter(collection.urlFilters);
        }

        public void add(String surt, SolrInputDocument doc) throws IOException, SolrServerException {
            if (filter.accepts(surt)) {
                server.add(doc, COMMIT_WITHIN_MS);
            }
        }
    }

    boolean anyFilterAccepts(List<Solr> solrs, String surt) {
        for (Solr solr : solrs) {
            if (solr.filter.accepts(surt)) {
                return true;
            }
        }
        return false;
    }

    void indexWarc(Db.Warc warc) {
        System.out.println(new Date() +  " Solr indexing " + warc.id + " " + warc.path);

        List<Solr> solrs = new ArrayList<>();

        try (Db db = dbPool.take()) {
            Db.Crawl crawl = db.findCrawl(warc.crawlId);
            List<Db.CollectionWithFilters> collections = db.listCollectionsForCrawlSeries(crawl.crawlSeriesId);
            for (Db.CollectionWithFilters collection : collections) {
                if (collection.solrUrl != null && !collection.solrUrl.isEmpty()) {
                    solrs.add(new Solr(collection));
                }
            }
        }

        List<SolrInputDocument> batch = new ArrayList<>();
        try (ArchiveReader reader = ArchiveReaderFactory.get(warc.path.toFile())) {
            for (ArchiveRecord record : reader) {
                if (record.getHeader().getUrl() == null) continue;
                String surt = SURT.toSURT(Warcs.getCleanUrl(record.getHeader()));
                if (surt == null || !anyFilterAccepts(solrs, surt)) {
                    continue; // skip indexing records we're not going to accept anyway
                }

                SolrInputDocument doc = makeDoc(record);
                if (doc == null) continue;

                for (Solr solr : solrs) {
                    try {
                        solr.add(surt, doc);
                    } catch (RuntimeException e) {
                        System.err.println("Error indexing " + doc.get("id"));
                        throw e;
                    }
                }
            }
            try (Db db = dbPool.take()) {
                db.updateWarcSolrIndexed(warc.id, System.currentTimeMillis());
            }
            System.out.println(new Date() + " Finished Solr indexing " + warc.id + " " + warc.path);
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private final static Pattern WWW_PREFIX = Pattern.compile("^www[0-9]*\\.");

    public static SolrInputDocument makeDoc(ArchiveRecord record) throws IOException {
        ArchiveRecordHeader warcHeader = record.getHeader();
        if (!Warcs.isResponseRecord(warcHeader)) return null;

        String url = Warcs.getCleanUrl(warcHeader);
        String site = topPrivateDomain(url);
        HttpHeader httpHeader = HttpHeader.parse(record, url);
        if (httpHeader == null || httpHeader.status < 200 || httpHeader.status > 299) {
            return null;
        }

        String contentType = httpHeader.getCleanContentType();
        String arcDate = Warcs.getArcDate(warcHeader);

        if (contentType == null) {
            return null;
        }

        SolrInputDocument doc = new SolrInputDocument();
        String shardKey = site;
        doc.addField("id", shardKey + "!" + url + " " + arcDate);
        doc.addField("url", url);
        doc.addField("length", warcHeader.getContentLength());
        doc.addField("code", httpHeader.status);
        Instant instant = LocalDateTime.parse(arcDate, Warcs.arcDateFormat).atOffset(ZoneOffset.UTC).toInstant();
        doc.addField("date", Date.from(instant));
        doc.addField("site", site);
        doc.addField("type", contentType);

        String digest = (String) warcHeader.getHeaderValue("WARC-Payload-Digest");
        if (digest != null) {
            if (digest.startsWith("sha1:")) {
                digest = digest.substring(5);
            }
            doc.addField("sha1", digest);
        }

        switch (contentType) {
            case "text/html":
                return extractHtmlContent(record, doc);
            case "application/pdf":
                return extractPdfContent(record, doc);
            default:
                return null; // unknown type
        }
    }

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

    private static SolrInputDocument extractHtmlContent(ArchiveRecord record, SolrInputDocument doc) {
        try {
            BoundedInputStream in = new BoundedInputStream(record, MAX_DOC_SIZE);
            TextDocument textDoc = new BoilerpipeSAXInput(new InputSource(in)).getTextDocument();
            doc.addField("title", textDoc.getTitle());
            DefaultExtractor.INSTANCE.process(textDoc);
            doc.addField("content", textDoc.getText(true, true).replace("\uFFFF", ""));
            doc.addField("boiled", textDoc.getContent().replace("\uFFFF", ""));
            return doc;
        } catch (SAXException | BoilerpipeProcessingException | IllegalArgumentException e) {
            return null;
        }
    }

    private static SolrInputDocument extractPdfContent(ArchiveRecord record, SolrInputDocument doc) throws IOException {
        Path tmp = null;
        try {
            CharBuffer buf = CharBuffer.allocate(MAX_DOC_SIZE);
            PdfReader pdfReader;

            if (record.getHeader().getLength() > PDF_DISK_OFFLOAD_THRESHOLD) {
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
                // reached MAX_DOC_SIZE amount of content
            }
            buf.flip();
            doc.addField("title", record.getHeader().getUrl());
            doc.addField("content", buf.toString());
            return doc;
        } catch (InvalidPdfException | NoClassDefFoundError | RuntimeException | EOFException e) {
            return null; // invalid or encrypted pdf
        } catch (Error e) {
            if (e.getMessage().startsWith("Unable to process ToUnicode map")) {
                // pdf reader abuses java.lang.Error sometimes to indicate a parse error
                return null;
            } else {
                // let everything else (eg OutOfMemoryError) through
                throw e;
            }
        } finally {
            if (tmp != null) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    public static void main(String args[]) {
        for (String arg : args) {
            try (ArchiveReader reader = ArchiveReaderFactory.get(arg)) {
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
