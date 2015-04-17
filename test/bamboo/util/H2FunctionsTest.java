package bamboo.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class H2FunctionsTest {

    @Test
    public void test() {
        assertEquals("www", H2Functions.substringIndex("www.example.org", ".", 1));
        assertEquals("www.example", H2Functions.substringIndex("www.example.org", ".", 2));
        assertEquals("example.org", H2Functions.substringIndex("www.example.org", ".", -2));
        assertEquals("file", H2Functions.substringIndex("foo/bar/file", "/", -1));
        assertEquals("bar/file", H2Functions.substringIndex("foo/bar/file", "/", -2));
    }
}
