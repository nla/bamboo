package bamboo.task;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WarcsTest {

    @Test
    public void testRepairCorruptArcDate () {
        assertEquals("20000929004850", Warcs.repairCorruptArcDate("2000092900480050"));
        assertEquals("20000831155515", Warcs.repairCorruptArcDate("200008311500550015"));
    }
}
