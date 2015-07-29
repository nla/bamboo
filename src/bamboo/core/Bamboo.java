package bamboo.core;

import bamboo.io.HeritrixJob;
import bamboo.task.CdxIndexer;
import bamboo.task.SolrIndexer;
import bamboo.task.Taskmaster;
import bamboo.task.Warcs;
import bamboo.web.Main;
import doss.BlobStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Bamboo implements AutoCloseable {
    public final Config config;
    public final DbPool dbPool;
    public final PandasDbPool pandasDbPool;
    public final Taskmaster taskmaster;
    public final BlobStore blobStore;

    public Bamboo(Config config, DbPool dbPool) {
        this.config = config;
        this.dbPool = dbPool;
        this.pandasDbPool = null;
        this.taskmaster = new Taskmaster(config, dbPool);
        //blobStore = LocalBlobStore.open(config.getDossHome());
        blobStore = null; // coming soon
    }

    public Bamboo() {
        config = new Config();
        dbPool = new DbPool(config);
        dbPool.migrate();
        if (config.getPandasDbUrl() != null) {
            pandasDbPool = new PandasDbPool(config);
        } else {
            pandasDbPool = null;
        }
        this.taskmaster = new Taskmaster(config, dbPool);
        //blobStore = LocalBlobStore.open(config.getDossHome());
        blobStore = null; // coming soon
    }

    @Override
    public void close() {
        dbPool.close();
        if (pandasDbPool != null) {
            pandasDbPool.close();
        }
    }

    public long importHeritrixCrawl(String jobName, Long crawlSeriesId) {
        HeritrixJob job = HeritrixJob.byName(config.getHeritrixJobs(), jobName);
        long crawlId;
        try (Db db = dbPool.take()) {
            crawlId = db.createCrawl(jobName, crawlSeriesId, Db.IMPORTING);
        }
        taskmaster.startImporting();
        return crawlId;
    }

    public void insertWarc(long crawlId, String path) throws IOException {
        try (Db db = dbPool.take()) {
            Path p = Paths.get(path);
            long size = Files.size(p);
            String digest = Scrub.calculateDigest("SHA-256", p);
            long warcId = db.insertWarc(crawlId, path, p.getFileName().toString(), size, digest);
            System.out.println("Registered WARC " + warcId);
        }
    }

    public void runCdxIndexer() throws Exception {
        new CdxIndexer(dbPool).run();
    }

    public void runSolrIndexer() throws Exception {
        new SolrIndexer(dbPool).run();
    }

    public void refreshWarcStats() throws IOException {
        try (Db db = dbPool.take()) {
            db.refreshWarcStatsOnCrawls();
            db.refreshWarcStatsOnCrawlSeries();
        }
    }

    public void refreshWarcStatsFs() throws IOException {
        try (Db db = dbPool.take()) {
            for (Db.Warc warc : db.listWarcs()) {
                long size = Files.size(warc.path);
                System.out.println(warc.size + " -> " + size + " " + warc.id + " " + warc.path);
                db.updateWarcSize(warc.id, size);
            }
            db.refreshWarcStatsOnCrawls();
            db.refreshWarcStatsOnCrawlSeries();
        }
    }

    public static void main(String args[]) throws Exception {
        Bamboo bamboo = new Bamboo();
        if (args.length == 0)
            usage();
        switch (args[0]) {
            case "import":
                bamboo.importHeritrixCrawl(args[1], Long.parseLong(args[2]));
                break;
            case "insert-warc":
                for (int i = 2; i < args.length; i++) {
                    bamboo.insertWarc(Long.parseLong(args[1]), args[i]);
                }
                break;
            case "cdx-indexer":
                bamboo.runCdxIndexer();
                break;
            case "solr-indexer":
                bamboo.runSolrIndexer();
                break;
            case "recalc-crawl-times":
                bamboo.recalcCrawlTimes();
                break;
            case "refresh-warc-stats":
                bamboo.refreshWarcStats();
                break;
            case "refresh-warc-stats-fs":
                bamboo.refreshWarcStatsFs();
                break;
            case "server":
                Main.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "scrub":
                Scrub.scrub(bamboo);
                break;
            default:
                usage();
        }
    }

    /**
     * Update crawls with an appriximation of their start and end times based on the timestamp extracted from (W)ARC
     * filenames.  Bit of a migration hack to fill in the table without a full reindex.
     */
    public void recalcCrawlTimes() {
        Pattern p = Pattern.compile(".*-((?:20|19)[0-9]{12,15})-[0-9]{5}-.*");
        try (Db db = dbPool.take()) {
            for (Db.Warc warc : db.listWarcs()) {
                Matcher m = p.matcher(warc.filename);
                if (m.matches()) {
                    Date date = Warcs.parseArcDate(m.group(1).substring(0, 14));
                    db.conditionallyUpdateCrawlStartTime(warc.crawlId, date);
                    db.conditionallyUpdateCrawlEndTime(warc.crawlId, date);
                }
            }
        }
    }

    public static void usage() {
        System.out.println("Usage: bamboo <subcommand>");
        System.out.println("Bamboo admin tools");
        System.out.println("\nSub-commands:");
        System.out.println("  cdx-indexer                      - Run the CDX indexer");
        System.out.println("  import <jobName> <crawlSeriesId> - Import a crawl from Heritrix");
        System.out.println("  insert-warc <crawl-id> <paths>   - Register WARCs with a crawl");
        System.out.println("  recalc-crawl-times               - Fill approx crawl times based on warc filenames (migration hack)");
        System.out.println("  refresh-warc-stats               - Refresh warc stats tables");
        System.out.println("  refresh-warc-stats-fs            - Refresh warc stats tables based on disk");
        System.out.println("  server                           - Run web server");
        System.exit(1);
    }
}
