package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.StatusLine;
import org.archive.format.arc.ARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.Base32;
import org.archive.util.LaxHttpParser;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class CdxIndexJob implements Taskmaster.Job {
    final private DbPool dbPool;
    final private URL cdxServer = getCdxServerUrl();

    public CdxIndexJob(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    @Override
    public void run(Taskmaster.IProgressMonitor progress) throws IOException {
        ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try (Db db = dbPool.take()) {
            for (Db.Warc warc : db.findWarcsToCdxIndex()) {
                threadPool.submit(() -> {
                    try (Db db2 = dbPool.take()) {
                        System.out.println("CDX indexing " + warc.id + " " + warc.path);
                        buildCdx(warc.path);
                        db2.setWarcCdxIndexed(warc.id, System.currentTimeMillis());
                        System.out.println("Finished CDX indexing " + warc.id + " " + warc.path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                threadPool.shutdown();
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            threadPool.shutdownNow();
        }
    }

    final static DateTimeFormatter warcDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    final static DateTimeFormatter arcDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static String warcToArcDate(String warcDate) {
        if (warcDate.length() == 14) {
            return warcDate; // already an ARC date
        }
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
        try (ArchiveReader reader = ArchiveReaderFactory.get(warc.toFile())) {
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

        String warcType = (String)h.getHeaderValue("WARC-Type");
        if (warcType != null && !warcType.equals("response"))
            return null;
        if (h.getUrl().startsWith("dns:") || h.getUrl().startsWith("filedesc:"))
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
        if (digest == null) {
            digest = calcDigest(record);
        } else if (digest.startsWith("sha1:")) {
            digest = digest.substring(5);
        }

        StringBuilder out = new StringBuilder();
        out.append('-').append(' '); // let server do canonicalization
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

    private static String calcDigest(ArchiveRecord record) throws IOException {
        String digest;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            byte[] buf = new byte[8192];
            for (; ; ) {
                int len = record.read(buf);
                if (len < 0) break;
                md.update(buf, 0, len);
            }
            digest = Base32.encode(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return digest;
    }

    private static String optional(String s) {
        if (s == null) {
            return "-";
        }
        return s;
    }

    private static String cleanContentType(String contentType) {
        if (contentType == null) return null;
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

    public static void main(String args[]) {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));
        writeCdx(Paths.get(args[0]), out);
    }
}
