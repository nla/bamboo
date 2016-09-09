package bamboo.task;

import bamboo.task.HttpHeader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HttpHeaderTest {
    @Test
    public void testParseStatusLine() {
        assertEquals(200, HttpHeader.parseStatusLine("HTTP/1.0 200 test"));
        assertEquals(200, HttpHeader.parseStatusLine("HTTP/1.0 200\r\n"));
        assertEquals(200, HttpHeader.parseStatusLine("HTTP/1.0 200\r"));
        assertEquals(200, HttpHeader.parseStatusLine("HTTP/1.0 200"));
        assertEquals(500, HttpHeader.parseStatusLine("HTTP/1.0    500  something wacky\r"));
    }
}
