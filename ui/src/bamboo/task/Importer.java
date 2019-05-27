package bamboo.task;

import bamboo.core.Config;
import bamboo.core.LockManager;
import bamboo.crawl.Crawl;
import bamboo.crawl.Crawls;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class Importer implements Runnable {
    final Crawls crawls;
    private Config config;
    private final LockManager lockManager;

    public Importer(Config config, Crawls crawls, LockManager lockManager) {
        this.config = config;
        this.crawls = crawls;
        this.lockManager = lockManager;
    }

    @Override
    public void run() {
        List<Crawl> candidates = crawls.listByStateId(Crawl.IMPORTING);
        for (Crawl crawl : candidates) {
            String lockName = "import-" + crawl.getId();
            if (lockManager.takeLock(lockName)) {
                try {
                    new ImportJob(config, crawls, crawl.getId()).run();
                } finally {
                    lockManager.releaseLock(lockName);
                }
            }
        }
    }
}
