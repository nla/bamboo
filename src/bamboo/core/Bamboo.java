package bamboo.core;

import bamboo.io.HeritrixJob;
import bamboo.task.CdxIndexJob;
import bamboo.task.ImportJob;
import bamboo.task.SolrIndexer;
import bamboo.task.Taskmaster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static droute.Response.render;
import static droute.Response.response;

public class Bamboo implements AutoCloseable {
    public final Config config;
    public final DbPool dbPool;
    public final Taskmaster taskmaster = new Taskmaster();

    public Bamboo(Config config, DbPool dbPool) {
        this.config = config;
        this.dbPool = dbPool;
    }

    public Bamboo() {
        config = new Config();
        dbPool = new DbPool(config);
        dbPool.migrate();
    }

    @Override
    public void close() {
        dbPool.close();
    }

    public Future<?> importHeritrixCrawl(String jobName, Long crawlSeriesId) {
        HeritrixJob job = HeritrixJob.byName(config.getHeritrixJobs(), jobName);
        long crawlId;
        try (Db db = dbPool.take()) {
            crawlId = db.createCrawl(jobName, crawlSeriesId);
        }
        ImportJob importJob = new ImportJob(config, dbPool, crawlId);
        return taskmaster.launch(importJob);
    }

    public void insertWarc(long crawlId, String path) {
        try (Db db = dbPool.take()) {
            long warcId = db.insertWarc(crawlId, path);
            System.out.println("Registered WARC " + warcId);
        }
    }

    public void runCdxIndexer() throws Exception {
        taskmaster.launch(new CdxIndexJob(dbPool)).get();
    }

    public void runSolrIndexer() throws Exception {
        new SolrIndexer(config, dbPool).run();
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
            default:
                usage();
        }
    }

    public static void usage() {
        System.out.println("Usage: bamboo <subcommand>");
        System.out.println("Bamboo admin tools");
        System.out.println("\nSub-commands:");
        System.out.println("  cdx-indexer                      - Run the CDX indexer");
        System.out.println("  import <jobName> <crawlSeriesId> - Import a crawl from Heritrix");
        System.out.println("  insert-warc <crawl-id> <paths>   - Register WARCs with a crawl");
        System.exit(1);
    }
}
