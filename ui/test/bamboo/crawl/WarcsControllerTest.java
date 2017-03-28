package bamboo.crawl;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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

    @Test
    public void testGsonDateHandling() throws ParseException {
        String utcString = "2004-08-21T15:30:36Z";
        // Turn this into a timezone specific date
        SimpleDateFormat sdfIn = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdfIn.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = sdfIn.parse(utcString);

        // Round trip test... sanity checking test logic has correctly understood timezones
        SimpleDateFormat sdfOut = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        sdfOut.setTimeZone(TimeZone.getTimeZone("Australia/ACT"));
        String output = sdfOut.format(date);
        String actTime = "2004-08-22T01:30:36+1000";
        assertEquals("Basic parse test failed. Test is wrong", actTime, output);

        // Now confirm GSON is using the correct time. It should include a timezone
        // Before fixing WA-7 it was labelling the string as UTC, but it was really local time.
        String gsonDate = WarcsController.gson.toJson(date);
        assertEquals("GSON date parsing is not configured correctly", "\"2004-08-22T01:30:36+10:00\"", gsonDate);
    }
}
