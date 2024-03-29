package bamboo.core;

import bamboo.api.ApiDAO;
import bamboo.crawl.*;
import bamboo.seedlist.SeedlistsDAO;
import bamboo.task.TaskDAO;
import org.jdbi.v3.sqlobject.*;

public interface DAO {
	// api package
	@CreateSqlObject ApiDAO api();

	// crawl package
	@CreateSqlObject AgencyDAO agency();
	@CreateSqlObject CollectionsDAO collections();
	@CreateSqlObject CrawlsDAO crawls();
	@CreateSqlObject SeriesDAO serieses();
	@CreateSqlObject WarcsDAO warcs();

	// seedlists package
	@CreateSqlObject SeedlistsDAO seedlists();

	// tasks package
	@CreateSqlObject
	TaskDAO tasks();

	@CreateSqlObject
    LockManagerDAO lockManager();
}
