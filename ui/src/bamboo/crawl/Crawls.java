package bamboo.crawl;

import bamboo.AuthHelper;
import bamboo.core.NotFoundException;
import bamboo.util.Pager;
import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.SizedWritable;
import org.apache.commons.io.IOUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.*;
import java.util.Collection;
import java.util.Collections;

public class Crawls {
    private final CrawlsDAO dao;
    private final Serieses serieses;
    private final Warcs warcs;
    private final BlobStore blobStore;

    private Set<CrawlStateListener> stateListeners = new HashSet<>();

    public Crawls(CrawlsDAO crawlsDAO, Serieses serieses, Warcs warcs, BlobStore blobStore) {
        this.dao = crawlsDAO;
        this.serieses = serieses;
        this.warcs = warcs;
        this.blobStore = blobStore;
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
        long crawlId = dao.createCrawl(jobName, crawlSeriesId, Crawl.IMPORTING, AuthHelper.currentUser());
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
            warcs.add(Warcs.fromFile(path));
        }

        return create(metadata, warcs, Collections.emptyList());
    }

    private long create(Crawl metadata, List<Warc> warcs, List<Artifact> artifacts) {
        metadata.setCreator(AuthHelper.currentUser());
        long id = dao.inTransaction((dao1, ts) -> {
            long totalBytes = warcs.stream().mapToLong(Warc::getSize).sum();
            long crawlId = dao.createCrawl(metadata);
            dao.warcs().batchInsertWarcsWithoutRollup(crawlId, warcs.iterator());
            int warcFilesDelta = warcs.size();
            dao.warcs().incrementWarcStatsForCrawlInternal(crawlId, warcFilesDelta, totalBytes);
            dao.warcs().incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, warcFilesDelta, totalBytes);
            dao.batchInsertArtifacts(crawlId, artifacts.iterator());
            return crawlId;
        });
        notifyStateChanged(id, Crawl.ARCHIVED);
        return id;
    }

    public void addWarcs(long crawlId, MultipartFile[] warcFiles) throws IOException {
        try (BlobTx tx = blobStore.begin()) {
            List<Warc> warcs = storeWarcs(tx, warcFiles);
            dao.inTransaction((dao1, ts) -> {
                long totalBytes = warcs.stream().mapToLong(Warc::getSize).sum();
                dao.warcs().batchInsertWarcsWithoutRollup(crawlId, warcs.iterator());
                int warcFilesDelta = warcs.size();
                dao.warcs().incrementWarcStatsForCrawlInternal(crawlId, warcFilesDelta, totalBytes);
                dao.warcs().incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, warcFilesDelta, totalBytes);
                return null;
            });
            tx.commit();
        }
    }

    public long create(Crawl crawl, MultipartFile[] warcFiles, MultipartFile[] artifactFiles) throws IOException {
        try (BlobTx tx = blobStore.begin()) {
            List<Warc> warcs = storeWarcs(tx, warcFiles);
            List<Artifact> artifacts = storeArtifacts(tx, artifactFiles);
            long crawlId = create(crawl, warcs, artifacts);
            tx.commit();
            return crawlId;
        }
    }

    private List<Artifact> storeArtifacts(BlobTx tx, MultipartFile[] artifactFiles) throws IOException {
        List<Artifact> artifacts = new ArrayList<>();
        for (MultipartFile file: artifactFiles) {
            if (file.isEmpty()) continue;

            Blob blob = tx.put(writable(file));
            Artifact artifact = new Artifact(blob, file.getOriginalFilename());
            artifacts.add(artifact);
        }
        return artifacts;
    }

    private List<Warc> storeWarcs(BlobTx tx, MultipartFile[] warcFiles) throws IOException {
        List<Warc> warcs = new ArrayList<>();

        for (MultipartFile warcFile: warcFiles) {
            if (warcFile.isEmpty()) continue; // blank form makes an empty file
            Blob blob = tx.put(writable(warcFile));
            warcs.add(Warcs.fromBlob(blob, warcFile.getOriginalFilename()));
        }
        return warcs;
    }

    private SizedWritable writable(MultipartFile warcFile) {
        return new SizedWritable() {
            @Override
            public void writeTo(WritableByteChannel writableByteChannel) throws IOException {
                try (InputStream stream = warcFile.getInputStream();
                     OutputStream out = Channels.newOutputStream(writableByteChannel)) {
                    IOUtils.copy(stream, out);
                }
            }

            @Override
            public long size() {
                return warcFile.getSize();
            }
        };
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

    public void recalculateWarcStats() {
        dao.recalculateWarcStats();
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

            int rows = dao.updateCrawl(crawlId, name, description, AuthHelper.currentUser());
            if (rows == 0) {
                throw new NotFoundException("crawl", crawlId);
            }
    }

    public Pager<CrawlAndSeriesName> pager(long page) {
            return new Pager<>(page, dao.countCrawls(), dao::paginateCrawlsWithSeriesName);
    }

    public Pager<Crawl> paginateWithSeriesId(long page, long seriesId) {
        return new Pager<>(page, dao.countCrawlsWithSeriesId(seriesId),
                (limit, offset) -> dao.paginateCrawlsWithSeriesId(seriesId, limit, offset));
    }

    public List<Crawl> listBySeriesId(long seriesId) {
            return dao.findCrawlsByCrawlSeriesId(seriesId);
    }

    public List<Crawl> listByStateId(int stateId) {
        return dao.findCrawlsByState(stateId);
    }

    public List<Artifact> listArtifacts(long crawlId) {
        return dao.listArtifacts(crawlId);
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

    public Crawl getByPandasInstanceIdOrNull(long instanceId) {
        return dao.findCrawlByPandasInstanceId(instanceId);
    }

    public void addArtifact(long crawlId, String type, Path path, String relpath) throws IOException {
        dao.createArtifact(crawlId, type, path, Files.size(path), Scrub.calculateDigest("SHA-256", path), relpath);
    }

    public Artifact getArtifact(long artifactId) {
        return NotFoundException.check(dao.findArtifact(artifactId), "artifact", artifactId);
    }

    public Artifact getArtifactByRelpath(long crawlId, String path) {
        return NotFoundException.check(getArtifactByRelpathOrNull(crawlId, path), "artifact", 0);
    }

    public Artifact getArtifactByRelpathOrNull(long crawlId, String path) {
        return dao.findArtifactByRelpath(crawlId, path);
    }

    public InputStream openArtifactStream(Artifact artifact) throws IOException {
        if (artifact.getBlobId() != null) {
            return blobStore.get(artifact.getBlobId()).openStream();
        } else if (artifact.getPath() != null) {
            return Files.newInputStream(artifact.getPath());
        } else {
            throw new IOException("Artifact has neither blobId nor path!");
        }
    }

    public List<Long> listPandasCrawlIds(long start) {
        return dao.listPandasCrawlIds(start);
    }
}
