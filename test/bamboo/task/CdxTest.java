package bamboo.task;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CdxTest {

    @Test
    public void testParseUrlMapLinePandas2() {
        Cdx.Alias alias = Cdx.parseUrlMapLine("14137/20050225-0000", "www.example.com/newsletters/2003/Jun/27%20Jun03-Newsletter16.pdf^^/14137/20050225/www.example.com/newsletters/2003/Jun/27");
        assertEquals("http://www.example.com/newsletters/2003/Jun/27%20Jun03-Newsletter16.pdf", alias.target);
        assertEquals("http://pandora.nla.gov.au/pan/14137/20050225-0000/www.example.com/newsletters/2003/Jun/27", alias.alias);
    }

    @Test
    public void testParseUrlMapLinePandas3() {
        Cdx.Alias alias = Cdx.parseUrlMapLine("14137/20130308-1342", "http://www.nationalswa.com/Portals/_default/default.css?cdv=63^^14137/20130308-1342/www.nationalswa.com/Portals/_default/defaultab4c.css");
        assertEquals("http://www.nationalswa.com/Portals/_default/default.css?cdv=63", alias.target);
        assertEquals("http://pandora.nla.gov.au/pan/14137/20130308-1342/www.nationalswa.com/Portals/_default/defaultab4c.css", alias.alias);
    }
}
