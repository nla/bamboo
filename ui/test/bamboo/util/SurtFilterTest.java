package bamboo.util;

import static org.junit.Assert.*;
import org.junit.Test;

public class SurtFilterTest {
    @Test
    public void emptyFilterShouldAccept() {
        assertTrue(new SurtFilter("").accepts("anything"));
    }

    @Test
    public void simpleFilterTests() {
        SurtFilter filter = new SurtFilter("-\n+(au,gov,\n-(au,gov,act, \n\n");
        assertFalse(filter.accepts("(com,example,)/index.html"));
        assertFalse(filter.accepts("(au,gov,act,www,)/"));
        assertTrue(filter.accepts("(au,gov,nla,www,)/fish.html"));
    }
}
