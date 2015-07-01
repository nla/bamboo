package bamboo.task;

import org.archive.url.SURT;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CdxIndexerTest {
    @Test
    public void stripScheme() {
        assertEquals("(au,gov,nla,www,", CdxIndexer.stripScheme(SURT.toSURT("http://www.nla.gov.au/")));
        assertEquals("(au,gov,nla,www", CdxIndexer.stripScheme(SURT.toSURT("https://www.nla.gov.au/")));
        assertEquals("(au,gov,nla,www", CdxIndexer.stripScheme("https://(au,gov,nla,www"));
        assertEquals("", CdxIndexer.stripScheme(""));
        assertEquals("www.nla.gov.au/", CdxIndexer.stripScheme("http://www.nla.gov.au/"));
    }

    @Test
    public void toSchemalessSURT() {
        assertEquals("(au,gov,nla,www,", CdxIndexer.toSchemalessSURT("http://www.nla.gov.au/"));
        assertEquals("(au,gov,nla,www,", CdxIndexer.toSchemalessSURT("https://www.nla.gov.au/"));
    }
}
