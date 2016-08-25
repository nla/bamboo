package bamboo.core;

import bamboo.crawl.*;
import bamboo.directory.CategoryDAO;
import bamboo.seedlist.SeedlistsDAO;
import org.skife.jdbi.v2.sqlobject.*;

public interface DAO {
	// crawl package
	@CreateSqlObject CollectionsDAO collections();
	@CreateSqlObject CrawlsDAO crawls();
	@CreateSqlObject SeriesDAO serieses();
	@CreateSqlObject WarcsDAO warcs();

	// directory package
	@CreateSqlObject
    CategoryDAO directory();

	// seedlists package
	@CreateSqlObject SeedlistsDAO seedlists();
}
