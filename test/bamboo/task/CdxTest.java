package bamboo.task;

import org.jsoup.select.Collector;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CdxTest {

    @Test
    public void testParseUrlMapLinePandas2() {
        List<Cdx.Alias> aliases = Cdx.parseUrlMapLine("www.example.com/newsletters/2003/Jun/27%20Jun03-Newsletter16.pdf^^/14137/20050225/www.example.com/newsletters/2003/Jun/27", "14137/20050225-0000").collect(Collectors.toList());
        assertEquals(2, aliases.size());
        assertEquals("http://www.example.com/newsletters/2003/Jun/27%20Jun03-Newsletter16.pdf", aliases.get(0).target);
        assertEquals("http://pandora.nla.gov.au/pan/14137/20050225-0000/www.example.com/newsletters/2003/Jun/27", aliases.get(0).alias);
        assertEquals(aliases.get(0).target, aliases.get(1).target);
        assertEquals("http://pandora.nla.gov.au/pan/14137/20050225-0000/www.example.com/newsletters/2003/Jun/27%20Jun03-Newsletter16.pdf", aliases.get(1).alias);
    }

    @Test
    public void testParseUrlMapLinePandas3() {
        List<Cdx.Alias> aliases = Cdx.parseUrlMapLine("http://www.nationalswa.com/Portals/_default/default.css?cdv=63^^14137/20130308-1342/www.nationalswa.com/Portals/_default/defaultab4c.css", "14137/20130308-1342").collect(Collectors.toList());
        assertEquals(2, aliases.size());
        assertEquals("http://www.nationalswa.com/Portals/_default/default.css?cdv=63", aliases.get(0).target);
        assertEquals("http://pandora.nla.gov.au/pan/14137/20130308-1342/www.nationalswa.com/Portals/_default/defaultab4c.css", aliases.get(0).alias);
        assertEquals(aliases.get(0).target, aliases.get(1).target);
        assertEquals("http://pandora.nla.gov.au/pan/14137/20130308-1342/www.nationalswa.com/Portals/_default/default.css?cdv=63", aliases.get(1).alias);
    }

    @Test
    public void testCleanHttrackPath() {
        String piAndDate = "1234/20010101-0000";
        assertEquals("example.org/index.html", Cdx.cleanHttrackPath("/1234/20010101-0000/example.org/index.html", piAndDate));
        assertEquals("example.org/index.html", Cdx.cleanHttrackPath("/1234/20010101/example.org/index.html", piAndDate));
        assertEquals("example.org/index.html", Cdx.cleanHttrackPath("/example.org/index.html", piAndDate));
        assertEquals("example.org/index.html", Cdx.cleanHttrackPath("1234/20010101-0000/example.org/index.html", piAndDate));
        assertEquals("example.org/index.html", Cdx.cleanHttrackPath("1234/20010101/example.org/index.html", piAndDate));
        assertEquals("example.org/index.html", Cdx.cleanHttrackPath("example.org/index.html", piAndDate));
    }
}
