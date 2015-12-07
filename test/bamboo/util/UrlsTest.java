package bamboo.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UrlsTest {

    @Test
    public void testRemoveScheme() {
        assertEquals("www.nla.gov.au", Urls.removeScheme("http://www.nla.gov.au"));
        assertEquals("www.nla.gov.au", Urls.removeScheme("www.nla.gov.au"));
    }
}
