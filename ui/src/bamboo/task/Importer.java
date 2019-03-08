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
    }

    @Override
    public void run() {
        List<Crawl> candidates = crawls.listByStateId(Crawl.IMPORTING);
        for (Crawl crawl : candidates) {
            new ImportJob(config, crawls, crawl.getId()).run();
        }
    }
}
