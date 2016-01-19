package bamboo.task;

import bamboo.core.Config;
import bamboo.core.Crawl;
import bamboo.core.Db;
import bamboo.core.DbPool;

import java.util.List;

public class Importer implements Runnable {
    final Config config;
    final DbPool dbPool;
    final Runnable completionHook;

    public Importer(Config config, DbPool dbPool, Runnable completionHook) {
        this.config = config;
        this.dbPool = dbPool;
        this.completionHook = completionHook;
    }

    @Override
    public void run() {
        for (;;) {
            List<Crawl> crawls;
            try (Db db = dbPool.take()) {
                crawls = db.findCrawlsByState(Db.IMPORTING);
            }
            if (crawls.isEmpty()) {
                break; // nothing left to do, go idle
            }
            for (Crawl crawl : crawls) {
                new ImportJob(config, dbPool, crawl.getId()).run();
                completionHook.run();
            }
        }
    }
}
