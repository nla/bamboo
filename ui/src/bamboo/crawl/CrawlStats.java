package bamboo.crawl;

import bamboo.util.Units;

public class CrawlStats {

    private final long warcsToBeCdxIndexed;
    private final long corruptWarcs;
    private final long artifactCount;
    private final long artifactBytes;

    CrawlStats(CrawlsDAO dao, long crawlId) {
        warcsToBeCdxIndexed = dao.warcs().countWarcsInCrawlAndState(crawlId, Warc.IMPORTED);
        corruptWarcs = dao.warcs().countWarcsInCrawlAndState(crawlId, Warc.CDX_ERROR);
        artifactCount = dao.getArtifactCount(crawlId);
        artifactBytes = dao.getArtifactBytes(crawlId);
    }

    public long getWarcsToBeCdxIndexed() {
        return warcsToBeCdxIndexed;
    }

    public long getCorruptWarcs() {
        return corruptWarcs;
    }

    public long getArtifactCount() {
        return artifactCount;
    }

    public long getArtifactBytes() {
        return artifactBytes;
    }
    public String getArtifactDisplaySize() {
        return Units.displaySize(getArtifactBytes());
    }
}
