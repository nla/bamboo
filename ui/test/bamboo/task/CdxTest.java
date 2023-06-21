package bamboo.task;

import org.junit.Test;
import org.netpreserve.jwarc.WarcReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.Assert.*;

public class CdxTest {
    @Test
    public void test() throws IOException {
        StringWriter stringWriter = new StringWriter();
        try (WarcReader warcReader = new WarcReader(Objects.requireNonNull(
                CdxTest.class.getResourceAsStream("example.warc.gz")))) {
            var stats = Cdx.buildIndex(warcReader, new PrintWriter(stringWriter), "example.warc.gz");
            assertEquals(2, stats.getRecords());
            assertEquals(5382, stats.getRecordBytes());
            assertEquals("warcprox 1.4", stats.getSoftware());
        }
    }
}