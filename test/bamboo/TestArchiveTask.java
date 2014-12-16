package bamboo;

import static java.nio.charset.StandardCharsets.*;
import static java.nio.file.StandardOpenOption.*;

import java.nio.file.*;

import org.junit.*;
import org.junit.rules.*;

public class TestArchiveTask {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	@Test
	public void test() throws Exception {
		Path jobPath = folder.newFolder("testcrawl").toPath();
		Files.write(jobPath.resolve("crawler-beans.cxml"), "test".getBytes(UTF_8), CREATE_NEW);
		Files.createDirectories(jobPath.resolve("20140801003348"));
		Files.createDirectories(jobPath.resolve("20140802011839"));
		ArchiveTask task = new ArchiveTask(jobPath);
		task.run();
		//fail("Not yet implemented");
	}

}
