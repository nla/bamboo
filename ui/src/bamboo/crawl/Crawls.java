package bamboo.crawl;

import bamboo.AuthHelper;
import bamboo.core.NotFoundException;
import bamboo.util.Pager;
import doss.*;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.springframework.security.access.prepost.PreAuthorize;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.*;
import java.util.stream.Collectors;

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

    public void addWarcs(long crawlId, List<NamedStream> warcFiles) throws IOException {
        try (BlobTx tx = blobStore.begin()) {
            List<Warc> warcs = storeWarcs(tx, warcFiles);
            int tries = 5;
            while (true) {
                try {
                    dao.inTransaction((dao1, ts) -> {
                        long totalBytes = warcs.stream().mapToLong(Warc::getSize).sum();
                        dao.warcs().batchInsertWarcsWithoutRollup(crawlId, warcs.iterator());
                        int warcFilesDelta = warcs.size();
                        dao.warcs().incrementWarcStatsForCrawlInternal(crawlId, warcFilesDelta, totalBytes);
                        dao.warcs().incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, warcFilesDelta, totalBytes);
                        return null;
                    });
                    break;
                } catch (UnableToExecuteStatementException e) {
                    if (e.getMessage().contains("try restarting transaction")) {
                        tries -= 1;
                        if (tries > 0) {
                            continue;
                        }
                    }
                    throw e;
                }
            }
            tx.commit();
        }
    }

    public void addArtifacts(long crawlId, List<NamedStream> streams) throws IOException {
        try (BlobTx tx = blobStore.begin()) {
            List<Artifact> artifacts = storeArtifacts(tx, streams);
            int tries = 5;
            while (true) {
                try {
                    dao.batchInsertArtifacts(crawlId, artifacts.iterator());
                    break;
                } catch (UnableToExecuteStatementException e) {
                    if (e.getMessage().contains("try restarting transaction")) {
                        tries -= 1;
                        if (tries > 0) {
                            continue;
                        }
                    }
                    throw e;
                }
            }
            tx.commit();
        }
    }

    public long createFromStreams(Crawl crawl, List<NamedStream> warcFiles, List<NamedStream> artifactFiles) throws IOException {
        try (BlobTx tx = blobStore.begin()) {
            List<Warc> warcs = storeWarcs(tx, warcFiles);
            List<Artifact> artifacts = storeArtifacts(tx, artifactFiles);
            long crawlId = create(crawl, warcs, artifacts);
            tx.commit();
            return crawlId;
        }
    }

    private List<Artifact> storeArtifacts(BlobTx tx, List<NamedStream> artifactFiles) throws IOException {
        List<Artifact> artifacts = new ArrayList<>();
        for (NamedStream file: artifactFiles) {
            Blob blob = tx.put(writable(file));
            Artifact artifact = new Artifact(blob, file.name());
            artifacts.add(artifact);
        }
        return artifacts;
    }

    private List<Warc> storeWarcs(BlobTx tx, List<NamedStream> warcFiles) throws IOException {
        List<Warc> warcs = new ArrayList<>();

        for (NamedStream warcFile: warcFiles) {
            Blob blob = tx.put(writable(warcFile));
            warcs.add(Warcs.fromBlob(blob, warcFile.name()));
        }
        return warcs;
    }

    private Writable writable(NamedStream warcFile) {
        if (warcFile.length() >= 0) {
            return new SizedWritable() {
                @Override
                public void writeTo(WritableByteChannel channel) throws IOException {
                    try (InputStream stream = warcFile.openStream()) {
                        stream.transferTo(Channels.newOutputStream(channel));
                    }
                }

                @Override
                public long size() {
                    return warcFile.length();
                }
            };
        } else {
            return channel -> {
                try (InputStream stream = warcFile.openStream()) {
                    stream.transferTo(Channels.newOutputStream(channel));
                }
            };
        }
    }


    public Crawl getOrNull(long crawlId) {
            return dao.findCrawl(crawlId);
    }

    /**
     * Retrieve a crawl's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    @PreAuthorize("hasPermission(#id, 'Crawl', 'view')")
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
    public void addWarcsFromPaths(long crawlId, List<Path> warcFiles) throws IOException {
        var streams = new ArrayList<NamedStream>();
        for (Path warcFile : warcFiles) {
            NamedStream of = NamedStream.of(warcFile);
            streams.add(of);
        }
        addWarcs(crawlId, streams);
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

    public Crawl getByWebrecorderCollectionId(String collectionId) {
        return NotFoundException.check(dao.findCrawlByWebrecorderCollectionId(collectionId), "crawl.webrecorderCollectionId", collectionId);
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

    public Statistics getArtifactStatistics() {
        return dao.getArtifactStatistics();
    }
}
