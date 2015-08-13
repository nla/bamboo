package bamboo.task;

import bamboo.core.Db;
import bamboo.core.DbPool;
import bamboo.core.Scrub;

import java.io.IOException;
import java.nio.file.*;
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Watches a directory for new WARC files.  As open (*.warc.gz.open) files are updated incrementally index them.  When
 * they're closed (renamed to *.warc.gz) finish importing them.
 */
public class WatchImporter {
    final static Logger log = Logger.getLogger(WatchImporter.class.getName());
    final Path dirToWatch;
    final DbPool dbPool;
    final long crawlId;

    public WatchImporter(DbPool dbPool, long crawlId, Path dirToWatch) {
        this.dbPool = dbPool;
        this.crawlId = crawlId;
        this.dirToWatch = dirToWatch;
    }

    public void run() throws IOException, InterruptedException {
        log.finest("watching " + dirToWatch);
        try (WatchService watcher = dirToWatch.getFileSystem().newWatchService()) {
            dirToWatch.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

            scanForChanges();

            for (WatchKey key = watcher.take(); key.isValid(); key = watcher.take()) {
                for (WatchEvent<?> event : key.pollEvents()) {
                    try {
                        if (event.kind() == OVERFLOW) {
                            scanForChanges();
                            continue;
                        }

                        Path path = dirToWatch.resolve((Path) event.context());
                        log.finest("saw event " + path);

                        if (!Files.exists(path)) {
                            /*
                             * Might have already been moved or renamed before we handled the event.  The file might
                             * still disappear at any later handling stage but let's skip it now if we can.
                             */
                            continue;
                        }

                        if (event.kind() == ENTRY_MODIFY && path.toString().endsWith(".warc.gz.open")) {
                            handleOpenWarc(path);
                        } else if (event.kind() == ENTRY_CREATE && path.toString().endsWith(".warc.gz")) {
                            handleClosedWarc(path);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                key.reset();
            }
        }
    }

    /**
     * Incrementally index any new records.
     */
    void handleOpenWarc(Path path) throws IOException {
        log.finest("handleOpenWarc(" + path + ")");
        long warcId, prevSize;
        long currentSize = Files.size(path);
        String filename = path.getFileName().toString().replaceFirst(".open$", "");
        try (Db db = dbPool.take()) {
            Db.Warc warc = db.findWarcByFilename(filename);
            if (warc != null) {
                warcId = warc.id;
                prevSize = warc.size;
            } else {
                /*
                 * Create the record under the final closed filename as its currently the key wayback uses to request
                 * a particular warc and we don't want to have to deal with it changing.
                 */
                warcId = db.insertWarc(crawlId, Db.Warc.OPEN, path.toString(), filename, 0L, null);
                prevSize = 0;
            }
        }
        if (currentSize > prevSize) {
            log.finest("Indexing " + warcId + " " + path);
            new CdxIndexer(dbPool).indexWarc(warcId);
            try (Db db = dbPool.take()) {
                db.updateWarcSize(crawlId, warcId, prevSize, currentSize);
            }
        }
    }

    /**
     * Move the WARC into the crawl's archival directory and finalise the db record.
     */
    private void handleClosedWarc(Path path) throws IOException {
        log.finest("handleClosedWarc(" + path + ")");
        Db.Warc warc;
        Db.Crawl crawl;
        try (Db db = dbPool.take()) {
            warc = db.findWarcByPath(path.toString());
            crawl = db.findCrawl(crawlId);
        }

        long size = Files.size(path);
        String digest = Scrub.calculateDigest("SHA-256", path);

        Path dest = moveWarcToCrawlDir(path, crawl);

        try (Db db = dbPool.take()) {
            if (warc == null) {
                db.insertWarc(crawlId, Db.Warc.IMPORTED, dest.toString(), path.getFileName().toString(), size, digest);
            } else {
                db.updateWarc(warc.crawlId, warc.id, Db.Warc.IMPORTED, dest.toString(), dest.getFileName().toString(), warc.size, size, digest);
            }
        }
    }

    private Path moveWarcToCrawlDir(Path path, Db.Crawl crawl) throws IOException {
        Path destDir = crawl.path.resolve(String.format("%03d", crawl.warcFiles / 1000));
        if (!Files.isDirectory(destDir)) {
            Files.createDirectory(destDir);
        }
        Path dest = destDir.resolve(path.getFileName());
        Files.move(path, dest);
        return dest;
    }

    /**
     * Scan the entire directory for any changes we might have missed.  We do this during startup or if the fs notify
     * event queue overflows.
     */
    private void scanForChanges() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirToWatch)) {
            for (Path entry : stream) {
                if (entry.toString().endsWith(".warc.gz.open")) {
                    handleOpenWarc(entry);
                } else if (entry.toString().endsWith(".warc.gz")) {
                    handleClosedWarc(entry);
                }
            }
        }
    }
}
