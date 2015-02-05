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
import org.archive.util.LaxHttpParser;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class CdxIndexJob implements Taskmaster.Job {
    final private DbPool dbPool;
    final private URL cdxServer = getCdxServerUrl();

    public CdxIndexJob(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    @Override
    public void run(Taskmaster.IProgressMonitor progress) throws IOException {
        try (Db db = dbPool.take()) {
            for (Db.Warc warc : db.findWarcsToCdxIndex()) {
                System.out.println("CDX indexing " + warc.id + " " + warc.path);
                buildCdx(warc.path);
                db.setWarcCdxIndexed(warc.id, System.currentTimeMillis());
            }
        }
    }

    final static DateTimeFormatter warcDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    final static DateTimeFormatter arcDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static String warcToArcDate(String warcDate) {
        return LocalDateTime.parse(warcDate, warcDateFormat).format(arcDateFormat);
    }

    private static URL getCdxServerUrl() {
        String cdxUrl = System.getenv("BAMBOO_CDX_URL");
        if (cdxUrl == null) {
            throw new IllegalStateException("Environment variable BAMBOO_CDX_URL must be set");
        }
        try {
            return new URL(cdxUrl);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildCdx(Path warc) throws IOException {
        StringWriter sw = new StringWriter();
        sw.write(" CDX N b a m s k r M S V g\n");
        writeCdx(warc, sw);
        byte[] data = sw.toString().getBytes(StandardCharsets.UTF_8);

        HttpURLConnection conn = (HttpURLConnection) cdxServer.openConnection();
        conn.setRequestMethod("POST");
        conn.addRequestProperty("Content-Type", "text/plain");
        conn.setFixedLengthStreamingMode(data.length);
        conn.setDoOutput(true);

        try (OutputStream out = conn.getOutputStream()) {
            out.write(data);
            out.flush();
        }

        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String output = rdr.readLine();
            int status = conn.getResponseCode();
            if (status != 200) {
                throw new RuntimeException("Indexing failed: " + output);
            }
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

    private static String formatCdxLine(String filename, ArchiveRecord record) throws IOException {
        ArchiveRecordHeader h = record.getHeader();

        if (!h.getHeaderValue("WARC-Type").equals("response"))
            return null;
        if (h.getUrl().startsWith("dns:"))
            return null;

        StatusLine status = null;
        String contentType = null;
        String location = null;

        // parse HTTP header
        String line = new String(LaxHttpParser.readRawLine(record), ISO_8859_1);
        if (StatusLine.startsWithHTTP(line)) {
            status = new StatusLine(line);
            for (Header header : LaxHttpParser.parseHeaders(record, ARCConstants.DEFAULT_ENCODING)) {
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

        contentType = cleanContentType(contentType);

        String digest = (String) h.getHeaderValue("WARC-Payload-Digest");
        if (digest != null && digest.startsWith("sha1:")) {
            digest = digest.substring(5);
        }

        StringBuilder out = new StringBuilder();
        out.append(h.getUrl()).append(' ');
        out.append(warcToArcDate(h.getDate())).append(' ');
        out.append(h.getUrl()).append(' ');
        out.append(optional(contentType)).append(' ');
        out.append(status == null ? "-" : Integer.toString(status.getStatusCode())).append(' ');
        out.append(optional(digest)).append(' ');
        out.append(optional(location)).append(' ');
        out.append("- "); // TODO: X-Robots-Tag http://noarchive.net/xrobots/
        out.append(Long.toString(h.getContentLength())).append(' ');
        out.append(Long.toString(h.getOffset())).append(' ');
        out.append(filename).append('\n');
        return out.toString();
    }

    private static String optional(String s) {
        if (s == null) {
            return "-";
        }
        return s;
    }

    private static String cleanContentType(String contentType) {
        contentType = stripAfterChar(contentType, ';');
        return stripAfterChar(contentType, ' ');
    }

    private static String stripAfterChar(String s, int c) {
        int i = s.indexOf(c);
        if (i > -1) {
            return s.substring(0, i);
        } else {
            return s;
        }
    }
}
