package bamboo.crawl;

import bamboo.core.NotFoundException;
import bamboo.util.Pager;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.Collection;

public class Crawls {
    private final CrawlsDAO dao;
    private final Serieses serieses;
    private final Warcs warcs;

    private Set<CrawlStateListener> stateListeners = new HashSet<>();

    public Crawls(CrawlsDAO crawlsDAO, Serieses serieses, Warcs warcs) {
        this.dao = crawlsDAO;
        this.serieses = serieses;
        this.warcs = warcs;
    }

    public void onStateChange(CrawlStateListener listener) {
        stateListeners.add(listener);
    }

    private void notifyStateChanged(long crawlId, int stateId) {
        for (CrawlStateListener listener : stateListeners) {
            listener.crawlStateChanged(crawlId, stateId);
        }
    }

    public long importHeritrixCrawl(String jobName, Long crawlSeriesId) {
        long crawlId = dao.createCrawl(jobName, crawlSeriesId, Crawl.IMPORTING);
        notifyStateChanged(crawlId, Crawl.IMPORTING);
        return crawlId;
    }

    /**
     * Create a crawl based on a set of existing WARC files without moving them.
     *
     * @return the id of the new crawl
     */
    public long createInPlace(Crawl metadata, Collection<Path> warcPaths) throws IOException {
        List<Warc> warcs = new ArrayList<>();
        for (Path path: warcPaths) {
            warcs.add(Warc.fromFile(path));
        }

        long id = dao.inTransaction((dao1, ts) -> {
            long totalBytes = warcs.stream().mapToLong(Warc::getSize).sum();
            long crawlId = dao.createCrawl(metadata);
            dao.warcs().batchInsertWarcsWithoutRollup(crawlId, warcs.iterator());
            int warcFilesDelta = warcs.size();
            dao.warcs().incrementWarcStatsForCrawlInternal(crawlId, warcFilesDelta, totalBytes);
            dao.warcs().incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, warcFilesDelta, totalBytes);
            return crawlId;
        });
        notifyStateChanged(id, Crawl.ARCHIVED);
        return id;
    }

    public Crawl getOrNull(long crawlId) {
            return dao.findCrawl(crawlId);
    }

    /**
     * Retrieve a crawl's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Crawl get(long crawlId) {
        return NotFoundException.check(getOrNull(crawlId), "crawl", crawlId);
    }

    /**
     * Retrieve various statistics about this crawl (number of WARC files etc).
     */
    public CrawlStats stats(long crawlId) {
            return new CrawlStats(dao, crawlId);
    }

    /**
     * Update a crawl
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public void update(long crawlId, String name, String description) {
        if (description != null && description.isEmpty()) {
            description = null;
        }

            int rows = dao.updateCrawl(crawlId, name, description);
            if (rows == 0) {
                throw new NotFoundException("crawl", crawlId);
            }
    }

    public Pager<CrawlAndSeriesName> pager(long page) {
            return new Pager<>(page, dao.countCrawls(), dao::paginateCrawlsWithSeriesName);
    }

    public List<Crawl> listBySeriesId(long seriesId) {
            return dao.findCrawlsByCrawlSeriesId(seriesId);
    }

    public List<Crawl> listByStateId(int stateId) {
        return dao.findCrawlsByState(stateId);
    }

    public void updateState(long crawlId, int stateId) {
        int rows = dao.updateCrawlState(crawlId, stateId);
        if (rows == 0) {
            throw new NotFoundException("crawl", crawlId);
        }
        notifyStateChanged(crawlId, stateId);
    }

    /**
     * Copies a collection of warc files into this crawl.
     */
    public void addWarcs(long crawlId, List<Path> warcFiles) throws IOException {
        // FIXME: handle failures
        Crawl crawl = get(crawlId);

        Path warcsDir = createWarcsDir(crawl);

        long i = crawl.getWarcFiles();
        for (Path src : warcFiles) {
            Path destDir = warcsDir.resolve(String.format("%03d", i++ / 1000));
            Path dest = destDir.resolve(src.getFileName());
            long size = Files.size(src);
            if (Files.exists(dest) && Files.size(dest) == size) {
                continue;
            }
            if (!Files.exists(destDir)) {
                Files.createDirectory(destDir);
            }
            String digest = Scrub.calculateDigest("SHA-256", src);
            Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);

            warcs.create(crawlId, Warc.IMPORTED, dest, dest.getFileName().toString(), size, digest);
        }
    }

    private Path createWarcsDir(Crawl crawl) throws IOException {
        Path warcsDir = allocateCrawlPath(crawl).resolve("warcs");
        if (!Files.exists(warcsDir)) {
            Files.createDirectory(warcsDir);
        }
        return warcsDir;
    }

    public Path allocateCrawlPath(long crawlId) throws IOException {
        Crawl crawl = get(crawlId);
        if (crawl.getPath() != null) {
            return crawl.getPath();
        }
        return allocateCrawlPath(crawl);
    }

    private Path allocateCrawlPath(Crawl crawl) throws IOException {
        Series series = serieses.get(crawl.getCrawlSeriesId());
        Path path;
        for (int i = 1;; i++) {
            path = series.getPath().resolve(String.format("%03d", i));
            try {
                Files.createDirectory(path);
                break;
            } catch (FileAlreadyExistsException e) {
                // try again
            }
        }

        dao.updateCrawlPath(crawl.getId(), path.toString());
        return path;
    }
}
