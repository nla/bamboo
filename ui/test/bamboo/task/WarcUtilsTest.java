package bamboo.task;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WarcUtilsTest {

    @Test
    public void testRepairCorruptArcDate () {
        assertEquals("20000929004850", WarcUtils.repairCorruptArcDate("2000092900480050"));
        assertEquals("20000831155515", WarcUtils.repairCorruptArcDate("200008311500550015"));
    }

    @Test
    public void testWarcToArcDate () {
        assertEquals("20000929004850", WarcUtils.warcToArcDate("2000-09-29T00:48:50Z"));
        assertEquals("20260707140015", WarcUtils.warcToArcDate("2026-07-07T14:00:15.998Z"));
    }
}
