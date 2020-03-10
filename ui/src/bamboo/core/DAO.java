package bamboo.core;

import bamboo.crawl.*;
import bamboo.seedlist.SeedlistsDAO;
import bamboo.task.TaskDAO;
import org.skife.jdbi.v2.sqlobject.*;

public interface DAO {
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
