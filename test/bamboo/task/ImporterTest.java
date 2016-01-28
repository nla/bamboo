package bamboo.task;

import bamboo.app.Bamboo;
import bamboo.core.Fixtures;
import bamboo.core.TestConfig;
import bamboo.crawl.Crawl;
import bamboo.core.DAO;
import bamboo.core.DbPool;
import bamboo.crawl.Crawls;
import bamboo.crawl.Serieses;
import org.junit.ClassRule;
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

	@ClassRule
	public static Fixtures fixtures = new Fixtures();


	@Test
	public void test() throws Exception {
		folder.create();
		TestConfig config = new TestConfig();
		config.setHeritrixJobs(folder.getRoot().toPath());
		DbPool dbPool = new DbPool(config);
		dbPool.migrate();

		Crawls crawls = new Crawls(fixtures.dao.crawls(), new Serieses(fixtures.dao.serieses()));

		Path seriesPath = folder.newFolder("crawl-series").toPath();
		long seriesId = fixtures.crawlSeriesId;

		Path jobPath = folder.newFolder("testcrawl").toPath();
		Files.write(jobPath.resolve("crawler-beans.cxml"), "test".getBytes(UTF_8), CREATE_NEW);
		Path launchWarcs = jobPath.resolve("20140801003348").resolve("warcs");
		Files.createDirectories(launchWarcs);
		Files.write(launchWarcs.resolve("TEST-1234.warc.gz"), "dummy".getBytes());

//		long crawlId = .createCrawl("testcrawl", seriesId, DAO.IMPORTING);
//		new ImportJob(config, dbPool, crawlId).run();
//
//		Crawl crawl = DAO.findCrawl(crawlId);
//		assertEquals(DAO.ARCHIVED, crawl.getState());
//		assertTrue(Files.exists(seriesPath.resolve("001/warcs/000/TEST-1234.warc.gz")));
	}

}
