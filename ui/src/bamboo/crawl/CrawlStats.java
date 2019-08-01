package bamboo.crawl;

public class CrawlStats {

    private final long warcsToBeCdxIndexed;
    private final long corruptWarcs;

    CrawlStats(CrawlsDAO dao, long crawlId) {
        warcsToBeCdxIndexed = dao.warcs().countWarcsInCrawlAndState(crawlId, Warc.IMPORTED);
        corruptWarcs = dao.warcs().countWarcsInCrawlAndState(crawlId, Warc.CDX_ERROR);
    }

    public long getWarcsToBeCdxIndexed() {
        return warcsToBeCdxIndexed;
    }

    public long getCorruptWarcs() {
        return corruptWarcs;
    }
}
