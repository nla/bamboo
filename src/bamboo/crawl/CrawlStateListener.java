package bamboo.crawl;

public interface CrawlStateListener {
    void crawlStateChanged(long crawlId, int stateId);
}
