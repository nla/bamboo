package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.StatusLine;
import org.archive.format.arc.ARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.url.WaybackURLKeyMaker;
import org.archive.util.LaxHttpParser;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class CdxIndexJob implements Taskmaster.Job {
    final private DbPool dbPool;
    final private long crawlId;

    public CdxIndexJob(DbPool dbPool, long crawlId) {
        this.dbPool = dbPool;
        this.crawlId = crawlId;
    }

    @Override
    public void run(Taskmaster.IProgressMonitor progress) throws IOException {
        Db.Crawl crawl;
        try (Db db = dbPool.take()) {
            crawl = db.findCrawl(crawlId);
            if (crawl == null)
                throw new RuntimeException("Crawl " + crawlId + " not found");
        }

        Path cdx = crawl.path.resolve("surt.cdx");
        Stream<Path> warcs = Files.walk(crawl.path.resolve("warcs"))
                .filter(path -> path.toString().endsWith(".warc.gz"));
        buildCdx(warcs, cdx);
    }

    final static DateTimeFormatter warcDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    final static DateTimeFormatter arcDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static String warcToArcDate(String warcDate) {
        return LocalDateTime.parse(warcDate, warcDateFormat).format(arcDateFormat);
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        buildCdx(Stream.of(args).skip(1).parallel().map(Paths::get), Paths.get(args[0]));
    }
    private static void buildCdx(Stream<Path> warcs, Path cdx) {
        ProcessBuilder pb = new ProcessBuilder("sort");
        pb.environment().put("LC_ALL", "C");
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(cdx.toFile());
        try {
            Process p = pb.start();
            try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(p.getOutputStream(), StandardCharsets.ISO_8859_1))) {
                out.write(" CDX N b a m s k r M S V g\n");
                warcs.forEach(warc -> writeCdx(warc, out));
                out.flush();
            }
            p.waitFor();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeCdx(Path warc, Writer out) {
        String filename = warc.getFileName().toString();
        try (WARCReader reader = WARCReaderFactory.get(warc.toFile())) {
            for (ArchiveRecord record : reader) {
                String cdxLine = formatCdxLine(filename, record);
                if (cdxLine != null) {
                    out.write(cdxLine);
                }
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final WaybackURLKeyMaker keyMaker = new WaybackURLKeyMaker(true);

    private static String formatCdxLine(String filename, ArchiveRecord record) throws IOException {
        ArchiveRecordHeader h = record.getHeader();

        if (!h.getHeaderValue("WARC-Type").equals("response"))
            return null;
        if (h.getUrl().startsWith("dns:"))
            return null;

        StringBuilder out = new StringBuilder();
        StatusLine status = null;
        String contentType = null;
        String location = null;

        // parse HTTP header
        Header[] headers;
        String line = new String(LaxHttpParser.readRawLine(record), ISO_8859_1);
        if (StatusLine.startsWithHTTP(line)) {
            status = new StatusLine(line);
            headers = LaxHttpParser.parseHeaders(record, ARCConstants.DEFAULT_ENCODING);
            for (Header header : headers) {
                switch (header.getName().toLowerCase()) {
                    case "location":
                        location = URI.create(h.getUrl()).resolve(header.getValue()).toString();
                        break;
                    case "content-type":
                        contentType = header.getValue();
                        break;
                }
            }
        }

        // massaged url
        try {
            out.append(keyMaker.makeKey(h.getUrl()));
        } catch (URISyntaxException e) {
            return null;
        }
        out.append(' ');

        // date
        out.append(warcToArcDate(h.getDate()));
        out.append(' ');

        // original url
        out.append(h.getUrl());
        out.append(' ');

        // content-type
        if (contentType != null) {
            out.append(contentType);
        } else {
            out.append('-');
        }
        out.append(' ');

        // response code
        if (status != null) {
            out.append(Integer.toString(status.getStatusCode()));
        } else {
            out.append('-');
        }
        out.append(' ');

        // new style checksum
        String digest = (String) h.getHeaderValue("WARC-Payload-Digest");
        if (digest == null) {
            digest = "-";
        } else if (digest.startsWith("sha1:")) {
            digest = digest.substring(5);
        }
        out.append(digest);
        out.append(' ');

        // redirect, currently unnecessary for wayback
        if (location != null) {
            out.append(location);
        } else {
            out.append('-');
        }
        out.append(' ');

        // TODO: X-Robots-Tag http://noarchive.net/xrobots/
        out.append("- ");

        // size
        out.append(Long.toString(h.getContentLength()));
        out.append(' ');

        // compressed arc file offset
        out.append(Long.toString(h.getOffset()));
        out.append(' ');

        // file name
        out.append(filename);
        out.append('\n');
        return out.toString();
    }
}
