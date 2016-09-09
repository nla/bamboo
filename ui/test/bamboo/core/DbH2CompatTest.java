package bamboo.core;

import bamboo.core.DbH2Compat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DbH2CompatTest {

    @Test
    public void test() {
        assertEquals("www", DbH2Compat.substringIndex("www.example.org", ".", 1));
        assertEquals("www.example", DbH2Compat.substringIndex("www.example.org", ".", 2));
        assertEquals("example.org", DbH2Compat.substringIndex("www.example.org", ".", -2));
        assertEquals("file", DbH2Compat.substringIndex("foo/bar/file", "/", -1));
        assertEquals("bar/file", DbH2Compat.substringIndex("foo/bar/file", "/", -2));
    }
}
