package bamboo.task;

import bamboo.TestConfig;
import bamboo.core.Bamboo;
import bamboo.core.Crawl;
import bamboo.core.Db;
import bamboo.core.DbPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ImporterTest {
	@Rule
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

			Path jobPath = folder.newFolder("testcrawl").toPath();
			Files.write(jobPath.resolve("crawler-beans.cxml"), "test".getBytes(UTF_8), CREATE_NEW);
			Path launchWarcs = jobPath.resolve("20140801003348").resolve("warcs");
			Files.createDirectories(launchWarcs);
			Files.write(launchWarcs.resolve("TEST-1234.warc.gz"), "dummy".getBytes());

			long crawlId = db.createCrawl("testcrawl", seriesId, Db.IMPORTING);
			new ImportJob(config, dbPool, crawlId).run();

			Crawl crawl = db.findCrawl(crawlId);
			assertEquals(Db.ARCHIVED, crawl.getState());
			assertTrue(Files.exists(seriesPath.resolve("001/warcs/000/TEST-1234.warc.gz")));
		}
	}

}
