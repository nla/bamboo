package bamboo.crawl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import bamboo.core.NotFoundException;
import bamboo.core.Streams;
import bamboo.util.Pager;
import doss.*;
import doss.http.HttpBlob;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.warc.WARCReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static java.nio.charset.StandardCharsets.*;

public class Warcs {
    private static final Logger log = LoggerFactory.getLogger(Warcs.class);

    private final WarcsDAO dao;
    private final BlobStore blobStore;
    private final String baseUrl;

    public Warcs(WarcsDAO warcsDAO) {
        this(warcsDAO, null, null);
    }

    public Warcs(WarcsDAO warcsDAO, BlobStore blobStore, String baseUrl) {
        this.dao = warcsDAO;
        this.blobStore = blobStore;
        this.baseUrl = baseUrl;
    }

    public List<Warc> findByCrawlId(long crawlId) {
        return dao.findWarcsByCrawlId(crawlId);
    }

    public Pager<Warc> paginateWithCrawlId(long page, long crawlId) {
        return new Pager<>(page, dao.countWarcsWithCrawlId(crawlId),
                (limit, offset) -> dao.paginateWarcsInCrawl(crawlId, limit, offset));
    }

    public Pager<Warc> paginateWithCrawlIdAndState(long page, long crawlId, int state) {
        return new Pager<>(page, dao.countWarcsInCrawlAndState(crawlId, state),
                (limit, offset) -> dao.paginateWarcsInCrawlAndState(crawlId, state, limit, offset));
    }

    public Pager<Warc> paginateWithState(long page, int stateId) {
        return new Pager<>(page, dao.countWarcsInState(stateId),
                (offset, limit) -> dao.paginateWarcsInState(stateId, offset, limit));
    }

    public Warc getOrNull(long id) {
        return dao.findWarc(id);
    }

    /**
     * Retrieve a series's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Warc get(long id) {
        return NotFoundException.check(getOrNull(id), "warc", id);
    }

    public Warc getOrNullByFilename(String filename) {
        return dao.findWarcByFilename(filename);
    }

    public Warc getByFilename(String filename) {
        return NotFoundException.check(getOrNullByFilename(filename), "warc with filename: " + filename, 0);
    }

    public Warc getByCrawlIdAndFilename(long crawlId, String filename) {
        return NotFoundException.check(getOrNullByCrawlIdAndFilename(crawlId, filename), "warc with filename: " + filename, 0);
    }

    public Warc getOrNullByCrawlIdAndFilename(long crawlId, String filename) {
        return dao.findWarcByCrawlIdAndFilename(crawlId, filename);
    }


    public String stateName(int stateId) {
        return dao.findWarcStateName(stateId);
    }

    @Deprecated
    public List<Warc> listAll() {
        return dao.listWarcs();
    }

    public void updateSha256(long warcId, String calculatedDigest) {
        int rows = dao.updateWarcSha256(warcId, calculatedDigest);
        if (rows == 0) {
            throw new NotFoundException("warc", warcId);
        }
    }

    public List<Warc> findByState(int stateId, int limit) {
        return dao.findWarcsInState(stateId, limit);
    }

    public void updateState(long id, int stateId) {
        dao.inTransaction((dao, ts) -> {
            updateState0(id, stateId);
            return null;
        });
    }

    private void updateState0(long id, int stateId) {
        int rows = dao.updateWarcStateWithoutHistory(id, stateId);
        if (rows == 0) {
            throw new NotFoundException("warc", id);
        }
        dao.insertWarcHistory(id, stateId);
    }

    public void updateRecordStats(long warcId, RecordStats stats, boolean deleteMode) {
        dao.inTransaction((dao, ts) -> {
            long records = stats.getRecords();
            long recordBytes = stats.getRecordBytes();
            if (deleteMode) {
                records = -records;
                recordBytes = -recordBytes;
            }
            dao.updateRecordStatsRollupForCrawl(warcId, records, recordBytes,
                    stats.getStartTime(), stats.getEndTime());
            dao.updateRecordStatsRollupForSeries(warcId, records, recordBytes,
                    stats.getStartTime(), stats.getEndTime());
            int rows = dao.updateWarcRecordStats(warcId, stats);
            if (rows == 0) {
                throw new NotFoundException("warc", warcId);
            }
            return null;
        });
    }

    public void updateCollections(long warcId, Map<Long, RecordStats> collectionStatsMap, boolean deleteMode) {
        for (Map.Entry<Long, RecordStats> entry : collectionStatsMap.entrySet()) {
            long collectionId = entry.getKey();
            RecordStats stats = entry.getValue();

            dao.inTransaction((dao, ts) -> {
                long recordsDelta = deleteMode ? 0 : stats.getRecords();
                long bytesDelta = deleteMode ? 0 : stats.getRecordBytes();

                CollectionWarc old = dao.selectCollectionWarcForUpdate(collectionId, warcId);
                if (old != null) {
                    recordsDelta -= old.records;
                    bytesDelta -= old.recordBytes;
                }

                dao.deleteCollectionWarc(collectionId, warcId);
                if (!deleteMode) {
                    dao.insertCollectionWarc(collectionId, warcId, stats.getRecords(), stats.getRecordBytes());
                    dao.incrementRecordStatsForCollection(collectionId, recordsDelta, bytesDelta);
                }

                return null;
            });
        }
    }

    public long create(long crawlId, int stateId, Path path, String filename, long size, String sha256) {
        return dao.inTransaction((dao, ts) -> {
            dao.incrementWarcStatsForCrawlInternal(crawlId, 1, size);
            dao.incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 1, size);
            long warcId = dao.insertWarcWithoutRollup(crawlId, stateId, path.toString(), filename, size, sha256);
            dao.insertWarcHistory(warcId, stateId);
            return warcId;
        });
    }

    public void updateSize(long warcId, long currentSize) {
        dao.inTransaction((dao, ts) -> {
            Warc prev = getAndLock(warcId);
            dao.updateWarcSizeWithoutRollup(warcId, currentSize);
            long crawlId = prev.getCrawlId();
            long sizeDelta = currentSize - prev.getSize();
            dao.incrementWarcStatsForCrawlInternal(crawlId, 0, sizeDelta);
            dao.incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 0, sizeDelta);
            return null;
        });
    }

    public void update(long warcId, int stateId, Path path, String filename, long size, String digest) {
        dao.inTransaction((dao, ts) -> {
            Warc prev = getAndLock(warcId);
            dao.updateWarcWithoutRollup(warcId, stateId, path.toString(), filename, size, digest);
            dao.insertWarcHistory(warcId, stateId);
            long crawlId = prev.getCrawlId();
            long sizeDelta = size - prev.getSize();
            dao.incrementWarcStatsForCrawlInternal(crawlId, 0, sizeDelta);
            dao.incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 0, sizeDelta);
            return null;
        });
    }

    private Warc getAndLock(long warcId) {
        return NotFoundException.check(dao.selectForUpdate(warcId), "warc", warcId);
    }

    public List<Warc> findByCollectionId(long collectionId, long start, long rows) {
        return dao.findByCollectionId(collectionId, start, rows);
    }

    public boolean healthcheck(PrintWriter out) {
        out.print("Check filesystem access to a WARC file... ");

        Warc warc = dao.findAnyWarc();
        if (warc == null) {
            out.println("UNKNOWN: no warcs in database");
            return true;
        }

        try (InputStream stream = openStream(warc)) {
            stream.read();
            out.println("OK");
            return true;
        } catch (IOException e) {
            out.println("ERROR: reading " + warc.getPath());
            e.printStackTrace(out);
            return false;
        }
    }

    public String moveToBlobStorage(long warcId, boolean deleteOriginal) throws IOException {
        Warc warc = get(warcId);
        if (warc.getBlobId() != null) return "already blob " + warc.getBlobId();
        if (warc.getPath() == null) return "failed - has no path";

        String sha256 = Scrub.calculateDigest("SHA-256", warc.getPath());
        if (warc.getSha256() != null && !warc.getSha256().equals(sha256)) {
            throw new RuntimeException("store digest for warc " + warcId + " doesn't match " + warc.getPath());
        }

        try (BlobTx tx = blobStore.begin()) {
            Blob blob = tx.put(warc.getPath());

            String blobSha256 = blob.digest("SHA-256");
            if (!sha256.equals(blobSha256)) throw new RuntimeException("blob digest doesn't match");

            int rows = dao.updateWarcBlobId(warc.getId(), blob.id());
            if (rows != 1) throw new RuntimeException("updating blob id failed");
            tx.commit();

            if (deleteOriginal) {
                try {
                    Files.deleteIfExists(warc.getPath());
                    dao.updateWarcPath(warc.getId(), null);
                } catch (IOException e) {
                    log.warn("moveToBlobStorage: unable to delete " + warc.getPath(), e);
                }
            }

            return "moved to blob " + blob.id();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static Warc fromFile(Path path) throws IOException {
        Warc warc = new Warc();
        warc.setPath(path);
        warc.setFilename(path.getFileName().toString());
        warc.setSize(Files.size(path));
        warc.setSha256(Scrub.calculateDigest("SHA-256", path));
        warc.setStateId(Warc.IMPORTED);
        return warc;
    }

    public static Warc fromBlob(Blob blob, String filename) throws IOException {
        if (filename == null || filename.isEmpty()) throw new IllegalArgumentException("filename is required");

        // workaround webrecorder naming .warc.gz files .warc
        if (filename.toLowerCase(Locale.ROOT).endsWith(".warc") && hasGzipSignature(blob)) {
            filename = filename + ".gz";
        }

        Warc warc = new Warc();
        warc.setBlobId(blob.id());
        warc.setFilename(filename);
        warc.setSize(blob.size());
        try {
            warc.setSha256(blob.digest("SHA-256"));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        warc.setStateId(Warc.IMPORTED);
        return warc;
    }

    private static boolean hasGzipSignature(Blob blob) throws IOException {
        try (InputStream stream = blob.openStream()) {
            return hasGzipSignature(stream);
        }
    }

    static boolean hasGzipSignature(InputStream stream) throws IOException {
        byte[] buf = new byte[2];
        int n = stream.read(buf);
        return n == 2 && (buf[0] & 0xff) == 0x1f && (buf[1] & 0xff) == 0x8b;
    }


    public List<WarcResumptionToken> resumptionByCollectionIdAndStateId(
            long collectionId, int stateAtLeast, WarcResumptionToken after, int limit) {
        return dao.resumptionByCollectionIdAndStateId(collectionId, stateAtLeast, Timestamp.from(after.time), after.id, limit);
    }


    private static AtomicInteger roundRobin = new AtomicInteger(0);

    public InputStream openStream(Warc warc) throws IOException {
        if (warc.getBlobId() != null) {
            return blobStore.get(warc.getBlobId()).openStream();
        }
        if (baseUrl != null) {
            return openStreamViaRoundRobinHttp(warc);
        }
        return Files.newInputStream(warc.getPath());
    }

    private InputStream openStreamViaRoundRobinHttp(Warc warc) throws IOException {
        URI uri = URI.create(baseUrl + warc.getPath());
        return openStreamViaRoundRobinHttp(uri);
    }

    public static InputStream openStreamViaRoundRobinHttp(URI uri) throws IOException {
        String host = uri.getHost();
        InetAddress[] addresses = InetAddress.getAllByName(host);
        if (addresses.length > 1) {
            int index = Math.floorMod(roundRobin.getAndIncrement(), addresses.length);
            String selectedHost = addresses[index].getHostAddress();
            try {
                uri = new URI(uri.getScheme(), uri.getUserInfo(), selectedHost, uri.getPort(), uri.getPath(),
                        uri.getQuery(), uri.getFragment());
            } catch (URISyntaxException e) {
                throw new IOException("building round-robin URL", e);
            }
        }

        URLConnection conn = uri.toURL().openConnection();
        if (uri.getUserInfo() != null) {
            String auth = Base64.getEncoder().encodeToString(uri.getUserInfo().getBytes(UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + auth);
        }

        if (addresses.length > 1) {
            conn.setRequestProperty("Host", host);
        }
        return conn.getInputStream();
    }

    public URI getUri(Warc warc) {
        return URI.create(baseUrl + warc.getPath());
    }

    public SeekableByteChannel openChannel(Warc warc) throws IOException {
        if (warc.getBlobId() != null) {
            return blobStore.get(warc.getBlobId()).openChannel();
        }
        return FileChannel.open(warc.getPath(), StandardOpenOption.READ);
    }

    public ArchiveReader openReader(Warc warc) throws IOException {
        return openReader(warc.getFilename(), openStream(warc));
    }

    public static ArchiveReader openReader(String filename, InputStream stream) throws IOException {
        /*
         * ArchiveReaderFactor.get doesn't understand the .open extension.
         */
        if (filename.endsWith(".warc.gz.open")) {
            return WARCReaderFactory.get(filename, stream, true);
        }
        return ArchiveReaderFactory.get(filename, stream, true);
    }

    public List<Warc> stream(long fromId, int limit) {
        return dao.streamWarcs(fromId, limit);
    }

    public List<Warc> streamSeries(long fromWarcId, long seriesId, int limit) {
        return dao.streamWarcsInSeries(fromWarcId, seriesId, limit);
    }

    public void copy(Warc warc, OutputStream outputStream, long start, long length) throws IOException {
        // optimisation for HttpBlob: use an exact range
        if (warc.getBlobId() != null) {
            Blob blob = blobStore.get(warc.getBlobId());
            if (blob instanceof HttpBlob) {
                try (InputStream in = ((HttpBlob) blob).openStream(start, length)) {
                    Streams.copy(in, outputStream);
                    return;
                }
            }
        }

        try (SeekableByteChannel in = openChannel(warc)) {
            in.position(start);
            Streams.copy(Channels.newInputStream(in), outputStream, length);
        }
    }

    public Statistics getStatistics() {
        return dao.getStatistics();
    }

    /**
     * Replaces the blob for the warc. The replacement must have the same size and SHA-256 digest.
     * This was only created to deal with some broken records caused by a DOSS1-DOSS2 migration
     * mistake and has no normal use case.
     */
    public boolean replaceCorruptBlob(Warc existing, InputStream inputStream, long size) throws IOException, NoSuchAlgorithmException {
        try {
            openStream(existing).read();
            return false;
        } catch (IOException e) {
            // OK.
        }

        try (BlobTx tx = blobStore.begin()) {
            Blob blob = tx.put(new SizedWritable() {
                @Override
                public long size() throws IOException {
                    return size;
                }

                @Override
                public void writeTo(WritableByteChannel writableByteChannel) throws IOException {
                    inputStream.transferTo(Channels.newOutputStream(writableByteChannel));
                }
            });

            String blobSha256 = blob.digest("SHA-256");
            if (!existing.getSha256().equals(blobSha256)) throw new RuntimeException("new digest doesn't match");

            dao.updateWarcBlobId(existing.getId(), blob.id());

            tx.commit();
            return true;
        }
    }
}
