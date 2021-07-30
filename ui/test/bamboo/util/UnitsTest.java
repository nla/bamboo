package bamboo.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class UnitsTest {
    @Test
    public void displaySize() {
        assertEquals("-9.5 MiB", Units.displaySize(-10000000));
        assertEquals("9.5 MiB", Units.displaySize(10000000));
        assertEquals("3.1 PiB", Units.displaySize(3521675312675137L));
    }
}