package bamboo.virus;

import bamboo.core.DAO;
import bamboo.core.DbPool;
import bamboo.core.LockManager;
import bamboo.core.TestConfig;
import bamboo.crawl.Crawl;
import bamboo.crawl.Crawls;
import bamboo.crawl.Series;
import bamboo.crawl.Serieses;
import bamboo.crawl.Warcs;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class VirusScannerTest {
    @Test
    public void completesAndCheckpointsAFullScanIncludingUnreadablePayloads() throws Exception {
        String dbUrl = "jdbc:h2:mem:virus-scanner-" + UUID.randomUUID() + ";mode=MySQL";
        TestConfig config = new TestConfig() {
            @Override
            public String getDbUrl() {
                return dbUrl;
            }
        };
        try (DbPool dbPool = new DbPool(config)) {
            dbPool.migrate();
            DAO dao = dbPool.dao();
            Warcs warcs = new Warcs(dao.warcs());
            Serieses serieses = new Serieses(dao.serieses());
            Crawls crawls = new Crawls(dao.crawls(), serieses, warcs, null);

            Series series = new Series();
            series.setName("virus scanner test");
            long seriesId = serieses.create(series);
            Crawl crawl = new Crawl();
            crawl.setName("virus scanner test");
            crawl.setCrawlSeriesId(seriesId);
            Path example = Path.of(getClass().getResource("/bamboo/task/example.warc.gz").toURI());
            crawls.createInPlace(crawl, List.of(example));

            Crawl secondCrawl = new Crawl();
            secondCrawl.setName("virus scanner test 2");
            secondCrawl.setCrawlSeriesId(seriesId);
            crawls.createInPlace(secondCrawl, List.of(example));

            CountDownLatch scansStarted = new CountDownLatch(2);
            AtomicInteger activeScans = new AtomicInteger();
            AtomicInteger maximumActiveScans = new AtomicInteger();
            AtomicInteger scanNumber = new AtomicInteger();

            ClamdClient cleanClamd = new ClamdClient(Path.of("unused")) {
                @Override
                public String version() {
                    return "ClamAV test/123";
                }

                @Override
                public ScanSession openSession() {
                    return new ScanSession() {
                        @Override
                        public ScanResult scan(InputStream input) throws IOException {
                            int active = activeScans.incrementAndGet();
                            maximumActiveScans.accumulateAndGet(active, Math::max);
                            scansStarted.countDown();
                            try {
                                if (!scansStarted.await(5, TimeUnit.SECONDS)) {
                                    throw new IOException("parallel scans did not start");
                                }
                                if (scanNumber.getAndIncrement() == 0) {
                                    throw new InputReadException(new EOFException("unexpected end of gzip stream"));
                                }
                                long bytes = input.transferTo(OutputStream.nullOutputStream());
                                return new ScanResult(Status.CLEAN, null, "stream: OK", bytes);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException(e);
                            } finally {
                                activeScans.decrementAndGet();
                            }
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            };

            try (LockManager locks = new LockManager(dao.lockManager())) {
                VirusScanner scanner = new VirusScanner(dao.virusScans(), warcs, locks, cleanClamd,
                        Duration.ofDays(7), 2);
                scanner.run(); // creates the run and scans the WARC
                VirusScanRun running = dao.virusScans().findRunningRun();
                assertEquals(running.getMaxWarcId(), running.getLastWarcId());
                assertEquals(2, maximumActiveScans.get());
                assertEquals(2L, dbPool.dbi.withHandle(handle -> handle
                        .createQuery("SELECT warcs_scanned FROM virus_scan_run WHERE id = :id")
                        .bind("id", running.getId()).mapTo(Long.class).one()).longValue());
                assertEquals("java.io.EOFException: unexpected end of gzip stream", dbPool.dbi.withHandle(handle -> handle
                        .createQuery("SELECT message FROM virus_scan_problem")
                        .mapTo(String.class).one()));

                scanner.run(); // observes the end of the high-water range and completes the run
                assertEquals("completed_with_errors", dao.virusScans().findLatestFinishedRun().getState());
            }
        }
    }
}
