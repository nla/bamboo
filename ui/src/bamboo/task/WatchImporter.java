package bamboo.task;

import bamboo.core.*;
import bamboo.crawl.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Watches a directory for new WARC files.  As open (*.warc.gz.open) files are updated incrementally index them.  When
 * they're closed (renamed to *.warc.gz) finish importing them.
 */
public class WatchImporter implements Runnable {
    final static Logger log = Logger.getLogger(WatchImporter.class.getName());
    final Map<Path,Config.Watch> watches = new HashMap<>();
    final Warcs warcs;
    final Crawls crawls;
    final Collections collections;
    final CdxIndexer cdxIndexer;

    public WatchImporter(Collections collections, Crawls crawls, CdxIndexer cdxIndexer, Warcs warcs, List<Config.Watch> watches) {
        this.collections = collections;
        this.crawls = crawls;
        this.cdxIndexer = cdxIndexer;
        this.warcs = warcs;
        for (Config.Watch watch: watches) {
            this.watches.put(watch.dir, watch);
        }
    }

    public void run() {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {


            for (Config.Watch watch : watches.values()) {
                log.info("Watching " + watch.dir + " for modified WARCs");
                watch.dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            }

            scanForChanges();

            for (WatchKey key = watcher.take(); key.isValid(); key = watcher.take()) {
                Config.Watch watch = watches.get(key.watchable());
                if (watch == null) {
                    log.warning("Ignoring unexpected watch key " + key.watchable());
                    continue;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    try {
                        if (event.kind() == OVERFLOW) {
                            scanForChanges();
                            continue;
                        }

                        Path path = watch.dir.resolve((Path) event.context());
                        log.finest("saw event " + path);

                        if (!Files.exists(path)) {
                            /*
                             * Might have already been moved or renamed before we handled the event.  The file might
                             * still disappear at any later handling stage but let's skip it now if we can.
                             */
                            continue;
                        }

                        if (event.kind() == ENTRY_MODIFY && path.toString().endsWith(".warc.gz.open")) {
                            handleOpenWarc(watch, path);
                        } else if (event.kind() == ENTRY_CREATE && path.toString().endsWith(".warc.gz")) {
                            handleClosedWarc(watch, path);
                        }
                    } catch (IOException | UncheckedIOException e) {
                        e.printStackTrace();
                    }
                }

                key.reset();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Incrementally index any new records.
     */
    void handleOpenWarc(Config.Watch watch, Path path) throws IOException {
        log.finest("handleOpenWarc(" + path + ")");
        long warcId, prevSize;
        long currentSize = Files.size(path);
        String filename = path.getFileName().toString().replaceFirst(".open$", "");
        Warc warc = warcs.getOrNullByFilename(filename);
        if (warc != null) {
            warcId = warc.getId();
            prevSize = warc.getSize();
        } else {
            /*
             * Create the record under the final closed filename as its currently the key wayback uses to request
             * a particular warc and we don't want to have to deal with it changing.
             */
            warcId = warcs.create(watch.crawlId, Warc.OPEN, path, filename, 0L, null);
            prevSize = 0;
        }
        if (currentSize > prevSize) {
            log.info("Indexing " + warcId + " " + path);
            cdxIndexer.indexWarc(warcId);
            warcs.updateSize(warcId, currentSize);
        }
    }

    /**
     * Move the WARC into the crawl's archival directory and finalise the db record.
     */
    private void handleClosedWarc(Config.Watch watch, Path path) throws IOException {
        log.finest("handleClosedWarc(" + path + ")");

        long size = Files.size(path);
        if (size == 0) {
            return; // ignore empty files
        }

        // Pywb flocks open files instead of renaming them, so check for a file lock
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            var lock = channel.tryLock();
            if (lock == null) {
                log.finest("WARC has file lock, treating as still open: " + path);
                handleOpenWarc(watch, path);
                return;
            }
            lock.release();
        }

        String filename = path.getFileName().toString();
        Warc warc = warcs.getOrNullByFilename(filename);
        Crawl crawl = crawls.get(watch.crawlId);

        String digest = Scrub.calculateDigest("SHA-256", path);

        log.info("Moving now-closed WARC " + path);
        Path dest = moveWarcToCrawlDir(path, crawl);

        if (warc == null) {
            warcs.create(watch.crawlId, Warc.IMPORTED, dest, filename, size, digest);
        } else {
            warcs.update(warc.getId(), Warc.IMPORTED, dest, filename, size, digest);
        }
    }

    private Path moveWarcToCrawlDir(Path path, Crawl crawl) throws IOException {
        Path destDir = crawl.getPath().resolve(String.format("%03d", crawl.getWarcFiles() / 1000));
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
        for (Config.Watch watch : watches.values()) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(watch.dir)) {
                for (Path entry : stream) {
                    if (entry.toString().endsWith(".warc.gz.open")) {
                        handleOpenWarc(watch, entry);
                    } else if (entry.toString().endsWith(".warc.gz")) {
                        handleClosedWarc(watch, entry);
                    }
                }
            }
        }
    }
}
