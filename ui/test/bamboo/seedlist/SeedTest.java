package bamboo.seedlist;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SeedTest {
    @Test
    public void testGetSurt() {

        assertEquals("(org,example,", new Seed("http://example.org").getSurt());
        assertEquals("(org,example,", new Seed("https://example.org").getSurt());
    }
}
