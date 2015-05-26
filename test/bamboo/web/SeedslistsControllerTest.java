package bamboo.web;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SeedslistsControllerTest {
    @Test
    public void testToSURT() {

        assertEquals("(org,example,", SeedlistsController.toProtocolAgnosticSURT("http://example.org"));
        assertEquals("(org,example,", SeedlistsController.toProtocolAgnosticSURT("https://example.org"));
    }
}
