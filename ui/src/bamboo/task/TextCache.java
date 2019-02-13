package bamboo.task;

import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TextCache {
    private static final Logger log = LoggerFactory.getLogger(CdxCache.class);
    private final Path root;
    private Warcs warcs;

    public TextCache(Path root, Warcs warcs) {
        this.root = root;
        this.warcs = warcs;
    }

    private Path entryPath(long warcId) {
        String dirs = "";
        for (long x = warcId / 1000; x > 0; x /= 1000) {
            dirs = String.format("%03d/%s", x % 1000, dirs);
        }
        return root.resolve(dirs).resolve("nla.warc-" + warcId + ".test.json.gz");
    }

    void populate(Warc warc) throws IOException {
        Path path = entryPath(warc.getId());
        if (Files.exists(path)) {
            return;
        }
        Files.createDirectories(path.getParent());
        Path tmpPath = Paths.get(path.toString() + ".tmp");
        try {
            // we run the text extractor as a child process so that when we hit a PDF or something that causes
            // tika to run out of memory we don't crash the controlling process
            ProcessBuilder pb = new ProcessBuilder("java", "-Xmx2048m", "bamboo.task.TextCache",
                    warcs.getUri(warc).toString(), tmpPath.toString());
            pb.environment().put("CLASSPATH", System.getProperty("java.class.path"));
            pb.inheritIO();
            Process p = pb.start();

            try {
                int x = p.waitFor();
                if (x != 0) {
                    throw new IOException("child returned error-code " + x);
                }
            } catch (InterruptedException e) {
                p.destroyForcibly();
                throw new IOException(e);
            }
            Files.move(tmpPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmpPath);
        }
    }

    public void populateAll(long startId, long endId) {
        long lastId = startId;
        AtomicLong count = new AtomicLong(0);

        while (lastId < endId) {
            List<Warc> list = warcs.stream(lastId, 1000);
            if (list.isEmpty()) {
                break;
            }

            list.parallelStream().forEach(warc -> {
                if (warc.getId() >= endId) {
                    return;
                }
                populate(count, warc);
            });

            lastId = list.get(list.size() - 1).getId();
        }
    }

    private void populate(AtomicLong count, Warc warc) {
        try {
            populate(warc);
        } catch (Exception e) {
            log.error("Error text extracting warc " + warc.getId() + " " + warc.getFilename(), e);
        }
        long progress = count.incrementAndGet();
        System.out.println(progress);
    }

    public void populateSeries(long seriesId) {
        long lastId = -1;
        AtomicLong count = new AtomicLong(0);

        while (true) {
            List<Warc> list = warcs.streamSeries(lastId, seriesId, 1000);
            if (list.isEmpty()) {
                break;
            }

            list.parallelStream().forEach(warc -> {
                populate(count, warc);
            });

            lastId = list.get(list.size() - 1).getId();
        }
    }

    public static void main(String args[]) throws IOException {
        URI uri = URI.create(args[0]);
        Path outPath = Paths.get(args[1]);
        String filename = uri.getPath().replaceFirst("^.*/", "");
        try (ArchiveReader reader = Warcs.openReader(filename, Warcs.openStreamViaRoundRobinHttp(uri));
             OutputStream out = new GZIPOutputStream(Files.newOutputStream(outPath), 8192)) {
            TextExtractor.extract(reader, out);
        }
    }


}
