package bamboo.task;

import bamboo.app.Bamboo;
import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.servers.Server;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Component
@ConditionalOnProperty("WARC_TEXT_CACHE")
public class TextCache {
    private static final Logger log = LoggerFactory.getLogger(CdxCache.class);
    private static final Gson gson = new Gson();
    private final Path root;
    private Warcs warcs;
    private final TextExtractor extractor;

    public static class Metadata {
        long warcId;
        long size;
        int stateId;
        String sha256;
    }

    public TextCache(@Value("${WARC_TEXT_CACHE}") Path root, Bamboo wa) throws IOException {
        this.root = root;
        this.warcs = wa.warcs;
        extractor = wa.textExtractor;
        log.info("TextCache at {}", root);
        if (Files.exists(root)) {
            Files.createDirectories(root);
        }
    }

    public Path entryPath(long warcId) {
        String dirs = "";
        for (long x = warcId / 1000; x > 0; x /= 1000) {
            dirs = String.format("%03d/%s", x % 1000, dirs);
        }
        return root.resolve(dirs).resolve("nla.warc-" + warcId + ".text.json.gz");
    }

    public Path metadataPath(long warcId) {
        return Paths.get(entryPath(warcId).toString() + ".meta.json");
    }

    private Path rankedPath(long warcId) {
        return Paths.get(entryPath(warcId).toString().replace("text", "ranked"));
    }

    public Metadata readMetadata(long warcId) throws IOException {
        Path path = metadataPath(warcId);
        if (!Files.exists(path)) {
            return null;
        }
        try (var reader = new InputStreamReader(Files.newInputStream(path))) {
            return gson.fromJson(reader, Metadata.class);
        } catch (Exception e) {
            log.warn("Deleting unreadable cache metadata {}", path, e);
            Files.deleteIfExists(path);
            return null;
        }
    }

    public boolean isCurrent(Warc warc) throws IOException {
        Path cachePath = find(warc.getId());
        if (cachePath == null) {
            return false;
        }

        Metadata metadata = readMetadata(warc.getId());
        if (metadata == null) {
            // Legacy cache entries are safe to keep for immutable closed WARCs.
            return warc.getStateId() != Warc.OPEN && warc.getSha256() != null;
        }

        if (metadata.warcId != warc.getId()) {
            return false;
        }
        if (metadata.size != warc.getSize()) {
            return false;
        }
        if (metadata.sha256 != null && warc.getSha256() != null && !metadata.sha256.equals(warc.getSha256())) {
            return false;
        }
        return true;
    }

    public void invalidate(long warcId) throws IOException {
        Files.deleteIfExists(entryPath(warcId));
        Files.deleteIfExists(rankedPath(warcId));
        Files.deleteIfExists(metadataPath(warcId));
    }

    public void writeMetadata(Warc warc) throws IOException {
        Metadata metadata = new Metadata();
        metadata.warcId = warc.getId();
        metadata.size = warc.getSize();
        metadata.stateId = warc.getStateId();
        metadata.sha256 = warc.getSha256();

        Path path = metadataPath(warc.getId());
        Path tmpPath = Paths.get(path.toString() + ".tmp");
        Files.createDirectories(path.getParent());
        try {
            try (var writer = new OutputStreamWriter(Files.newOutputStream(tmpPath))) {
                gson.toJson(metadata, writer);
            }
            Files.move(tmpPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmpPath);
        }
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
                 OutputStream out = new GZIPOutputStream(Files.newOutputStream(tmpPath), 8192)) {
                extractor.extractAll(reader, out);
            }
            Files.move(tmpPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
            writeMetadata(warc);
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


    public Path find(long warcId) {
        Path path = entryPath(warcId);
        Path rankedPath = rankedPath(warcId);
        if (Files.exists(rankedPath)) {
            return rankedPath;
        }
        if (Files.exists(path)) {
            return path;
        }
        return null;
    }
}
