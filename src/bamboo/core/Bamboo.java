package bamboo.core;

import bamboo.io.HeritrixJob;
import bamboo.task.CdxIndexJob;
import bamboo.task.ImportJob;
import bamboo.task.Taskmaster;

import java.io.IOException;
import java.io.UncheckedIOException;
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

    public static void main(String args[]) {
        Bamboo bamboo = new Bamboo();
        if (args.length == 0)
            usage();
        switch (args[0]) {
            case "import":
                bamboo.importHeritrixCrawl(args[1], Long.parseLong(args[2]));
                break;
            default:
                usage();
        }
    }

    public static void usage() {
        System.out.println("Usage: bamboo <subcommand>");
        System.out.println("\nSub-commands:");
        System.out.println("  import <jobName> <crawlSeriesId> s- Import a crawl from Heritrix");
        System.exit(1);
    }

    public void buildCdx(long crawlId)  {
        CdxIndexJob job = new CdxIndexJob(dbPool, crawlId);
        taskmaster.launch(job);
    }
}
