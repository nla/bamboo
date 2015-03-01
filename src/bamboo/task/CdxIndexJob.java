package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.url.SURT;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
                        db2.updateWarcCdxIndexed(warc.id, System.currentTimeMillis());
                        System.out.println("Finished CDX indexing " + warc.id + " " + warc.path);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
            }
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            threadPool.shutdownNow();
        }
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
        if (!Warcs.isResponseRecord(h)) {
            return null;
        }
        String url = Warcs.getCleanUrl(h);
        HttpHeader http = HttpHeader.parse(record, url);
        if (http == null) {
            return null;
        }

        StringBuilder out = new StringBuilder();
        out.append('-').append(' '); // let server do canonicalization
        out.append(Warcs.getArcDate(h)).append(' ');
        out.append(url).append(' ');
        out.append(optional(http.getCleanContentType())).append(' ');
        out.append(http.status == -1 ? "-" : Integer.toString(http.status)).append(' ');
        out.append(optional(Warcs.getOrCalcDigest(record))).append(' ');
        out.append(optional(http.location)).append(' ');
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

    public static void main(String args[]) {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));
        writeCdx(Paths.get(args[0]), out);
    }
}
