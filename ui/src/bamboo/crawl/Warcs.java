package bamboo.crawl;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.*;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import bamboo.core.NotFoundException;
import bamboo.util.Pager;
import doss.BlobStore;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.warc.WARCReaderFactory;
import org.skife.jdbi.v2.sqlobject.Bind;

import static java.nio.charset.StandardCharsets.*;

public class Warcs {
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

    public void updateRecordStats(long warcId, RecordStats stats) {
        dao.inTransaction((dao, ts) -> {
            dao.updateRecordStatsRollupForCrawl(warcId, stats);
            dao.updateRecordStatsRollupForSeries(warcId, stats);
            int rows = dao.updateWarcRecordStats(warcId, stats.getRecords(), stats.getRecordBytes());
            if (rows == 0) {
                throw new NotFoundException("warc", warcId);
            }
            return null;
        });
    }

    public void updateCollections(long warcId, Map<Long, RecordStats> collectionStatsMap) {
        for (Map.Entry<Long, RecordStats> entry : collectionStatsMap.entrySet()) {
            long collectionId = entry.getKey();
            RecordStats stats = entry.getValue();

            dao.inTransaction((dao, ts) -> {
                long recordsDelta = stats.getRecords();
                long bytesDelta = stats.getRecordBytes();

                CollectionWarc old = dao.selectCollectionWarcForUpdate(collectionId, warcId);
                if (old != null) {
                    recordsDelta -= old.records;
                    bytesDelta -= old.recordBytes;
                }

                dao.deleteCollectionWarc(collectionId, warcId);
                dao.insertCollectionWarc(collectionId, warcId, stats.getRecords(), stats.getRecordBytes());
                dao.incrementRecordStatsForCollection(collectionId, recordsDelta, bytesDelta);

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

    public static Warc fromFile(Path path) throws IOException {
        Warc warc = new Warc();
        warc.setPath(path);
        warc.setFilename(path.getFileName().toString());
        warc.setSize(Files.size(path));
        warc.setSha256(Scrub.calculateDigest("SHA-256", path));
        warc.setStateId(Warc.IMPORTED);
        return warc;
    }

    public List<WarcResumptionToken> resumptionByCollectionIdAndStateId(
            long collectionId, int stateId, WarcResumptionToken after, int limit) {
        return dao.resumptionByCollectionIdAndStateId(collectionId, stateId, Timestamp.from(after.time), after.id, limit);
    }


    private AtomicInteger roundRobin = new AtomicInteger(0);

    public InputStream openStream(Warc warc) throws IOException {
        if (warc.getBlobId() != null) {
            return blobStore.get(warc.getBlobId()).openStream();
        }
        if (baseUrl != null) {
            URI uri = URI.create(baseUrl + warc.getPath());
            String host = uri.getHost();
            InetAddress[] addresses = InetAddress.getAllByName(host);
            if (addresses.length > 1) {
                try {
                    String robinHost = addresses[roundRobin.getAndIncrement() % addresses.length].toString();
                    uri = new URI(uri.getScheme(), uri.getUserInfo(), robinHost, uri.getPort(), uri.getPath(),
                            uri.getQuery(), uri.getFragment());
                } catch (URISyntaxException e) {
                    throw new IOException(e);
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
        return Files.newInputStream(warc.getPath());
    }

    public SeekableByteChannel openChannel(Warc warc) throws IOException {
        if (warc.getBlobId() != null) {
            return blobStore.get(warc.getBlobId()).openChannel();
        }
        return FileChannel.open(warc.getPath(), StandardOpenOption.READ);
    }

    public ArchiveReader openReader(Warc warc) throws IOException {
        /*
         * ArchiveReaderFactor.get doesn't understand the .open extension.
         */
        if (warc.getFilename().endsWith(".warc.gz.open")) {
            return WARCReaderFactory.get(warc.getFilename(), openStream(warc), true);
        }
        return ArchiveReaderFactory.get(warc.getFilename(), openStream(warc), true);
    }
}
