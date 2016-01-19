package bamboo.core;

public class CrawlStats {

    private final long warcsToBeCdxIndexed;
    private final long warcsToBeSolrIndexed;
    private final long corruptWarcs;

    CrawlStats(Db db, long crawlId) {
        warcsToBeCdxIndexed = db.countWarcsInCrawlAndState(crawlId, Warc.IMPORTED);
        warcsToBeSolrIndexed = db.countWarcsInCrawlAndState(crawlId, Warc.CDX_INDEXED);
        corruptWarcs = db.countWarcsInCrawlAndState(crawlId, Warc.CDX_ERROR);
    }

    public long getWarcsToBeCdxIndexed() {
        return warcsToBeCdxIndexed;
    }

    public long getWarcsToBeSolrIndexed() {
        return warcsToBeSolrIndexed;
    }

    public long getCorruptWarcs() {
        return corruptWarcs;
    }
}
