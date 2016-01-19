package bamboo.core;

import bamboo.core.WarcsController;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WarcsControllerTest {
    @Test
    public void testParseRange() {
        assertNull(WarcsController.Range.parseHeader("bogus", 10));
        assertNull(WarcsController.Range.parseHeader("bytes=", 10));
        List<WarcsController.Range> ranges = WarcsController.Range.parseHeader("bytes=1-2,2-3,-4", 10);
        assertEquals(3, ranges.size());
        assertEquals(1, ranges.get(0).start);
        assertEquals(2, ranges.get(0).length);
        assertEquals(10, ranges.get(0).total);
        assertEquals("1-2/10", ranges.get(0).toString());
        assertEquals("2-3/10", ranges.get(1).toString());
        assertEquals("6-9/10", ranges.get(2).toString());
    }
}
