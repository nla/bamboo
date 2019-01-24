package bamboo.task;

import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import bamboo.util.Pager;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.ThreadPool;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;

public class CdxCache {
    private static final Logger log = LoggerFactory.getLogger(CdxCache.class);
    private final Path root;
    private Warcs warcs;

    public CdxCache(Path root, Warcs warcs) {
        this.root = root;
        this.warcs = warcs;
    }

    private Path entryPath(long warcId) {
        String dirs = "";
        for (long x = warcId / 1000; x > 0; x /= 1000) {
            dirs = (x % 1000) + "/" + dirs;
        }
        return root.resolve(dirs).resolve(warcId + ".cdx.gz");
    }

    void populate(Warc warc) throws IOException {
        Path path = entryPath(warc.getId());
        if (Files.exists(path)) {
            return;
        }
        Files.createDirectories(path.getParent());
        Path tmpPath = Paths.get(path.toString() + ".tmp");
        try {
            try (ArchiveReader reader = warcs.openReader(warc);
                 Writer writer = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(tmpPath, CREATE), 8192), UTF_8)) {
                Cdx.writeCdx(reader, warc.getFilename(), warc.getSize(), writer);
            }
            Files.move(tmpPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmpPath);
        }
    }

    public void populateAll() {
        long lastId = -1;
        AtomicLong count = new AtomicLong(0);

        while (true) {
            List<Warc> list = warcs.stream(lastId, 1000);
            if (list.isEmpty()) {
                break;
            }

            list.parallelStream().forEach(warc -> {
                try {
                    populate(warc);
                } catch (IOException e) {
                    log.error("Error indexing warc " + warc.getId() + " " + warc.getFilename(), e);
                }
                long progress = count.incrementAndGet();
                System.out.println(progress);
            });

            lastId = list.get(list.size() - 1).getId();
        }
    }
}
