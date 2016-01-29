package bamboo.pandas;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PandasInstanceTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testWarcFiles() throws IOException {
        Path warcDir = tmp.newFolder("warc").toPath();
        Path titleDir = warcDir.resolve("010").resolve("10001");

        Files.createDirectories(titleDir);

        Path warc1 = titleDir.resolve("nla.arc-10001-20140101-0000-000.warc.gz");
        Path warc2 = titleDir.resolve("nla.arc-10001-20140101-0000-001.warc.gz");

        Files.createFile(titleDir.resolve("decoy.txt"));
        Files.createFile(warc1);
        Files.createFile(warc2);
        Files.createFile(titleDir.resolve("nla.arc-10001-20140101-0000-000.cdx"));
        Files.createFile(titleDir.resolve("nla.arc-10001-20150202-0000-000.warc.gz"));

        PandasInstance instance = new PandasInstance(warcDir, 42, 10001, "20140101-0000", "Test title");
        List<Path> warcFiles = instance.warcFiles();
        assertEquals(new HashSet(Arrays.asList(warc1, warc2)), new HashSet(warcFiles));
    }
}
