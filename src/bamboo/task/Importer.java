package bamboo.task;

import bamboo.core.Config;
import bamboo.crawl.Crawl;
import bamboo.crawl.Crawls;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class Importer implements Runnable {
    final Crawls crawls;
    private Config config;

    public Importer(Config config, Crawls crawls) {
        this.config = config;
        this.crawls = crawls;

        crawls.onStateChange((crawlId, stateId) -> {
            if (stateId == Crawl.IMPORTING) {
                synchronized (this) {
                    notify();
                }
            }
        });
    }

    @Override
    public void run() {
        try {
            for (;;) {
                List<Crawl> candidates = crawls.listByStateId(Crawl.IMPORTING);
                if (candidates.isEmpty()) {
                    synchronized (this) {
                        TimeUnit.MINUTES.timedWait(this, 1);
                    }
                    continue;
                }

                for (Crawl crawl : candidates) {
                    new ImportJob(config, crawls, crawl.getId()).run();
                }
            }
        } catch (InterruptedException e) {
            // shutdown
        }
    }
}
