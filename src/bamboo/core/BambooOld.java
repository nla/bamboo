package bamboo.core;

import bamboo.crawl.*;
import bamboo.task.HeritrixJob;
import bamboo.task.*;
import doss.BlobStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BambooOld {
//    public final Config config;
//    private final DbPool dbPool;
//    public final PandasDbPool pandasDbPool;
//    public final Taskmaster taskmaster;
//    public final BlobStore blobStore;
//
//    public BambooOld(Config config, DbPool dbPool) {
//        this.config = config;
//        this.dbPool = dbPool;
//        if (config.getPandasDbUrl() != null) {
//            pandasDbPool = new PandasDbPool(config);
//        } else {
//            pandasDbPool = null;
//        }
//        this.taskmaster = new Taskmaster(config, dbPool);
//        //blobStore = LocalBlobStore.open(config.getDossHome());
//        blobStore = null; // coming soon
//    }
//
//    public BambooOld(Config config) {
//        this(config, new DbPool(config));
//    }
//
//    public BambooOld() {
//        this(new Config());
//    }
//
//    @Override
//    public void close() {
//        dbPool.close();
//        if (pandasDbPool != null) {
//            pandasDbPool.close();
//        }
//    }
//
//    public void insertWarc(long crawlId, String path) throws IOException {
//        try (DAO DAO = dbPool.take()) {
//            Path p = Paths.get(path);
//            long size = Files.size(p);
//            String digest = Scrub.calculateDigest("SHA-256", p);
//            long warcId = DAO.insertWarc(crawlId, Warc.IMPORTED, path, p.getFileName().toString(), size, digest);
//            System.out.println("Registered WARC " + warcId);
//        }
//    }
//
//    public void runCdxIndexer() throws Exception {
//        new CdxIndexer(dbPool).run();
//    }
//
//    public void runSolrIndexer() throws Exception {
//        new SolrIndexer(dbPool).run();
//    }
//
//    public void refreshWarcStats() throws IOException {
//        try (DAO DAO = dbPool.take()) {
//            DAO.refreshWarcStatsOnCrawls();
//            DAO.refreshWarcStatsOnCrawlSeries();
//        }
//    }
//
//    public void refreshWarcStatsFs() throws IOException {
//        try (DAO DAO = dbPool.take()) {
//            for (Warc warc : DAO.listWarcs()) {
//                long size = Files.size(warc.getPath());
//                System.out.println(warc.getSize() + " -> " + size + " " + warc.getId() + " " + warc.getPath());
//                DAO.updateWarcSizeWithoutRollup(warc.getId(), size);
//            }
//            DAO.refreshWarcStatsOnCrawls();
//            DAO.refreshWarcStatsOnCrawlSeries();
//        }
//    }
//
//    /**
//     * Update crawls with an appriximation of their start and end times based on the timestamp extracted from (W)ARC
//     * filenames.  Bit of a migration hack to fill in the table without a full reindex.
//     */
//    public void recalcCrawlTimes() {
//        Pattern p = Pattern.compile(".*-((?:20|19)[0-9]{12,15})-[0-9]{5}-.*");
//        try (DAO DAO = dbPool.take()) {
//            for (Warc warc : DAO.listWarcs()) {
//                Matcher m = p.matcher(warc.getFilename());
//                if (m.matches()) {
//                    Date date = WarcUtils.parseArcDate(m.group(1).substring(0, 14));
//                    DAO.conditionallyUpdateCrawlStartTime(warc.getCrawlId(), date);
//                    DAO.conditionallyUpdateCrawlEndTime(warc.getCrawlId(), date);
//                }
//            }
//        }
//    }
//
//    public void startWorkerThreads() {
//        startWatchImporter();
//    }
//
//    void startWatchImporter() {
//        List<Config.Watch> watches = config.getWatches();
//        if (!watches.isEmpty()) {
//            Thread thread = new Thread(()-> {
//                try {
//                    new WatchImporter(dbPool, watches).run();
//                } catch (IOException | InterruptedException e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            });
//            thread.setDaemon(true);
//            thread.start();
//        }
//    }
}
