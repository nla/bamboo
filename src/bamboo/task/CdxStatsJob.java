package bamboo.task;

import org.apache.commons.compress.utils.CountingInputStream;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import static java.nio.file.StandardOpenOption.READ;

public class CdxStatsJob implements Taskmaster.Job {
    final Path cdxPath;
    final static long taskSize = 8 * 1024 * 1024;
    List<CdxStatsTask> tasks = new ArrayList<>();

    public CdxStatsJob(Path cdxPath) throws IOException {
        this.cdxPath = cdxPath;
    }

    public void launch(ExecutorService executor) {
        executor.execute(() -> {
            try {
                ExecutorCompletionService<CdxStatsTask> queue = new ExecutorCompletionService<>(executor);
                long cdxSize = Files.size(cdxPath);
                Map<String,String> pathIndex = loadPathIndex();
                for (long start = 0; start < cdxSize; start += taskSize) {
                    CdxStatsTask task = new CdxStatsTask(pathIndex, cdxPath, start, taskSize);
                    queue.submit(task, task);
                    tasks.add(task);
                }

                long totalBytes = 0;
                long totalRecords = 0;
                for (int i = 0; i < tasks.size(); i++) {
                    CdxStatsTask task = queue.take().get();
                    totalBytes += task.totalBytes;
                    totalRecords += task.goodRecords;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                /* shutdown */
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private static HashMap<String, String> loadPathIndex() throws MalformedURLException, IOException {
        HashMap<String, String> map = new HashMap<>();
        Path pathIndex = Paths.get(System.getProperty("user.home"), "tmp", "agwa-path-index.txt");
        try (BufferedReader rdr = Files.newBufferedReader(pathIndex)) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                int i = line.indexOf(9);
                if (i < 0) continue;
                map.put(line.substring(0, i), line.substring(i + 1));
            }
        }
        return map;
    }

    @Override
    public String getProgress() {
        long targetBytes = 0;
        long progressBytes = 0;
        for (CdxStatsTask task : tasks) {
            targetBytes += task.length;
            progressBytes += task.cdxBytesRead;
        }
        double percent = 100.0 * progressBytes / targetBytes;
        return "Calculating CDX statistics: " + String.format("%.2f", percent) +
                "%  (" + progressBytes + " of " + targetBytes + " bytes)";
    }

    public static class CdxStatsTask implements Runnable {
        final Path cdxPath;
        final long start;
        final long length;
        long badRecords = 0;
        long goodRecords = 0;
        long totalBytes = 0;
        long cdxBytesRead = 0;
        Map<String,String> pathIndex;

        public CdxStatsTask(Map<String, String> pathIndex, Path cdxPath, long start, long length) {
            this.pathIndex = pathIndex;
            this.cdxPath = cdxPath;
            this.start = start;
            this.length = length;
        }

        @Override
        public void run() {
            try (CdxReader rdr = new CdxReader(cdxPath, start, length)) {
                for (CdxLine line = rdr.readLine(); line != null; line = rdr.readLine()) {
                    String warc = pathIndex.get(line.warc);
                    if (warc == null) {
                        System.err.println("WARC missing from path-index, skipping: " + line.warc);
                        continue;
                    }
                    try (FileChannel channel = FileChannel.open(Paths.get(warc), READ)) {
                        channel.position(line.offset);
                        GZIPInputStream gzStream = new GZIPInputStream(Channels.newInputStream(channel));
                        ArcHeader arcHeader = new ArcHeader();
                        arcHeader.parse(new DataInputStream(gzStream));
                        goodRecords++;
                        totalBytes += arcHeader.length;
                        cdxBytesRead = rdr.getBytesRead();
                    } catch (IOException e) {
                        badRecords++;
                        cdxBytesRead = rdr.getBytesRead();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class CdxReader implements Closeable {
        final SplitReader reader;
        long badLines = 0;
        long goodLines = 0;

        public CdxReader(Path cdxPath, long start, long length) throws IOException {
            reader = new SplitReader(cdxPath, start, length);
        }

        public CdxLine readLine() throws IOException {
            while (true) {
                String line = reader.readLine();
                if (line == null) return null;
                if (line.startsWith(" ")) continue;
                CdxLine cdxLine = CdxLine.parse(line);
                if (cdxLine != null) {
                    goodLines++;
                    return cdxLine;
                }
                badLines++;
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        public long getBytesRead() {
            return reader.getBytesRead();
        }
    }

    /**
     * Reads lines from a (size, length) window into a text file.
     */
    public static class SplitReader implements Closeable {
        final CountingInputStream cis;
        final DataInputStream dis;
        final long length;

        public SplitReader(Path path, long start, long length) throws IOException {
            this(Files.newByteChannel(path, READ), start, length);
        }

        public SplitReader(SeekableByteChannel channel, long start, long length) throws IOException {
            this.length = length;
            if (start > 0) {
                channel.position(start);
            }
            InputStream stream = new BufferedInputStream(Channels.newInputStream(channel));
            cis = new CountingInputStream(stream);
            dis = new DataInputStream(cis);
            if (start > 0) {
                // ensure we start on a line boundary
                dis.readLine();
            }
        }

        public String readLine() throws IOException {
            if (cis.getBytesRead() >= length) {
                return null;
            }
            return dis.readLine();
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }

        public long getBytesRead() {
            return cis.getBytesRead();
        }
    }

    public static class CdxLine {
        public final String warc;
        public final long offset;

        public CdxLine(String warc, long offset) {
            this.warc = warc;
            this.offset = offset;
        }

        public static CdxLine parse(String line) {
            int i = line.lastIndexOf(' ');
            if (i < 0)
                return null;
            int j = line.lastIndexOf(' ', i - 1);
            if (j < 0)
                return null;
            long offset = Long.parseLong(line.substring(j + 1, i));
            String warc = line.substring(i + 1);
            return new CdxLine(warc, offset);
        }
    }

    static class ArcHeader {
        long length = -1;
        String contentType = null;
        String url = null;
        String date = null;
        String digest = null;

        boolean isValid() {
            return url != null && date != null && length >= 0;
        }

        void parse(DataInputStream dis) throws IOException {
            String protocolLine = dis.readLine();
            if (protocolLine.startsWith("WARC/")) {
                parseWarcHeaders(dis);
            } else {
                parseArcHeader(protocolLine);
            }
        }

        private void parseArcHeader(String protocolLine) {
            String[] parts = protocolLine.split(" ");
            url = parts[0];
            date = parts[2];
            contentType = parts[3];
            length = Long.parseLong(parts[4]);
        }

        private void parseWarcHeaders(DataInputStream dis) throws IOException {
            String headerLine;
            while (!((headerLine = dis.readLine()) == null || headerLine.isEmpty())) {
                int i = headerLine.indexOf(':');
                if (i < 0) {
                    throw new IllegalArgumentException("broken WARC header");
                }
                String key = headerLine.substring(0, i);
                String value = headerLine.substring(i + 1).trim();

                switch (key) {
                case "Content-Length":
                    try {
                        length = Long.parseLong(value);
                    } catch (NumberFormatException e) {
                        // skip it
                    }
                    break;
                case "WARC-Target-URI":
                    url = value;
                    break;
                case "WARC-Date":
                    try {
                        date = this.arcDate.format(warcDate.parse(value));
                    } catch (ParseException e) {
                        // skip it
                    }
                    break;

                case "WARC-Payload-Digest":
                    digest = value;
                    break;
                }
            }
        }

        private static SimpleDateFormat constructWarcDateFormat() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            format.setLenient(false);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }

        private static SimpleDateFormat constructArcDateFormat() {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            format.setLenient(false);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }

        final SimpleDateFormat warcDate = constructWarcDateFormat();
        final SimpleDateFormat arcDate = constructArcDateFormat();
    }
}
