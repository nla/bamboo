package bamboo;

import static java.nio.charset.StandardCharsets.*;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.Assert.assertTrue;

import java.nio.file.*;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import bamboo.core.DbPool;
import bamboo.task.ImportJob;
import org.junit.*;
import org.junit.rules.*;

public class ArchiveTaskTest {
	//@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	@Test
	public void test() throws Exception {
		folder.create();
		TestConfig config = new TestConfig();
		config.setHeritrixJobs(folder.getRoot().toPath());
		DbPool dbPool = new DbPool(config);
		dbPool.migrate();
		Bamboo bamboo = new Bamboo(config, dbPool);

		Path seriesPath = folder.newFolder("crawl-series").toPath();
		long seriesId;
		try (Db db = dbPool.take()) {
			seriesId = db.createCrawlSeries("test series", seriesPath.toString());
		}

		Path jobPath = folder.newFolder("testcrawl").toPath();
		Files.write(jobPath.resolve("crawler-beans.cxml"), "test".getBytes(UTF_8), CREATE_NEW);
		Path launchWarcs = jobPath.resolve("20140801003348").resolve("warcs");
		Files.createDirectories(launchWarcs);
		Files.write(launchWarcs.resolve("TEST-1234.warc.gz"), "dummy".getBytes());
		//Files.createDirectories(jobPath.resolve("20140802011839"));

		bamboo.importHeritrixCrawl("testcrawl", seriesId).get();

		assertTrue(Files.exists(seriesPath.resolve("001/warcs/000/TEST-1234.warc.gz")));
		//fail("Not yet implemented");
	}

}
