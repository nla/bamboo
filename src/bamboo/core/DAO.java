package bamboo.core;

import bamboo.crawl.*;
import bamboo.directory.DirectoryDAO;
import bamboo.seedlist.SeedlistsDAO;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public interface DAO {
	// crawl package
	@CreateSqlObject CollectionsDAO collections();
	@CreateSqlObject CrawlsDAO crawls();
	@CreateSqlObject SeriesDAO serieses();
	@CreateSqlObject WarcsDAO warcs();

	// directory package
	@CreateSqlObject DirectoryDAO directory();

	// seedlists package
	@CreateSqlObject SeedlistsDAO seedlists();
}
