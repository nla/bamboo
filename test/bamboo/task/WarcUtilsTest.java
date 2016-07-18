package bamboo.task;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WarcUtilsTest {

    @Test
    public void testRepairCorruptArcDate () {
        assertEquals("20000929004850", WarcUtils.repairCorruptArcDate("2000092900480050"));
        assertEquals("20000831155515", WarcUtils.repairCorruptArcDate("200008311500550015"));
    }
}
