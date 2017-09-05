package bamboo.core;

import bamboo.crawl.*;
import bamboo.seedlist.SeedlistsDAO;
import org.skife.jdbi.v2.sqlobject.*;

public interface DAO {
	// crawl package
	@CreateSqlObject CollectionsDAO collections();
	@CreateSqlObject CrawlsDAO crawls();
	@CreateSqlObject SeriesDAO serieses();
	@CreateSqlObject WarcsDAO warcs();

	// seedlists package
	@CreateSqlObject SeedlistsDAO seedlists();

	@CreateSqlObject
    LockManagerDAO lockManager();
}
