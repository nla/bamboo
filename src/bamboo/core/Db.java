package bamboo.core;

import com.google.common.net.InternetDomainName;
import org.apache.commons.lang.StringEscapeUtils;
import org.archive.url.URLParser;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public abstract class Db implements AutoCloseable, Transactional {

	public abstract  void close();

	public static class Collection {
		public final long id;
		public final String name;
		public final String cdxUrl;
		public final String solrUrl;
		public final long records;
		public final long recordBytes;
		public final String description;

		public Collection(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			name = rs.getString("name");
			cdxUrl = rs.getString("cdx_url");
			solrUrl = rs.getString("solr_url");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
			description = rs.getString("description");
		}
	}

	public static class CollectionMapper implements ResultSetMapper<Collection> {
		@Override
		public Collection map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
			return new Collection(rs);
		}
	}

	public static class CollectionWithFilters extends Collection {
		public final String urlFilters;

		public CollectionWithFilters(ResultSet rs) throws SQLException {
			super(rs);
			urlFilters = rs.getString("url_filters");
		}
	}

	public static class CollectionWithFiltersMapper implements ResultSetMapper<CollectionWithFilters> {
		@Override
		public CollectionWithFilters map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
			return new CollectionWithFilters(rs);
		}
	}

	@SqlUpdate("SELECT COUNT(*) FROM collection")
	public abstract long countCollections();

	@SqlQuery("SELECT * FROM collection ORDER BY name")
	public abstract Iterable<Collection> listCollections();

	@SqlQuery("SELECT * FROM collection ORDER BY name LIMIT :limit OFFSET :offset")
	public abstract List<Collection> paginateCollections(@Bind("limit") long limit, @Bind("offset") long offset);

	@SqlUpdate("UPDATE collection SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
	public abstract int incrementRecordStatsForCollection(@Bind("id") long collectionId, @Bind("records") long records, @Bind("bytes") long bytes);

	@SqlQuery("SELECT collection.*, collection_series.url_filters FROM collection_series LEFT JOIN collection ON collection.id = collection_id WHERE crawl_series_id = :it")
	public abstract List<CollectionWithFilters> listCollectionsForCrawlSeries(@Bind long crawlSeriesId);

	@SqlUpdate("DELETE FROM collection_series WHERE crawl_series_id = :it")
	public abstract void removeCrawlSeriesFromAllCollections(@Bind long crawlSeriesId);

	@SqlBatch("INSERT INTO collection_series (crawl_series_id, collection_id, url_filters) VALUES (:crawl_series_id, :collection_id, :url_filters)")
	public abstract void addCrawlSeriesToCollections(@Bind("crawl_series_id") long crawlSeriesId, @Bind("collection_id") List<Long> collectionIds, @Bind("url_filters") List<String> urlFilters);

	@SqlQuery("SELECT * FROM collection WHERE id = :id")
	public abstract  Collection findCollection(@Bind("id") long id);

	@SqlUpdate("INSERT INTO collection(name, description, cdx_url, solr_url) VALUES (:name, :description, :cdxUrl, :solrUrl)")
	@GetGeneratedKeys
	public abstract long createCollection(@Bind("name")  String name, @Bind("description") String description, @Bind("cdxUrl") String cdxUrl, @Bind("solrUrl")  String solrUrl);

	@SqlUpdate("UPDATE collection SET name = :name, description = :description, cdx_url = :cdxUrl, solr_url = :solrUrl WHERE id = :id")
	public abstract int updateCollection(@Bind("id") long collectionId, @Bind("name")  String name, @Bind("description") String description, @Bind("cdxUrl") String cdxUrl, @Bind("solrUrl") String solrUrl);

	public static class CollectionWarc {
		public final long collectionId;
		public final long warcId;
		public final long records;
		public final long recordBytes;

		public CollectionWarc(ResultSet rs) throws SQLException {
			collectionId = rs.getLong("collection_id");
			warcId = rs.getLong("warc_id");
			records = rs.getLong("records");
			recordBytes = rs.getLong("recordBytes");
		}
	}

	@SqlQuery("SELECT * FROM collection_warc WHERE collection_id = :collectionId AND warc_id = :warcId")
	public abstract CollectionWarc findCollectionWarc(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId);

	@SqlUpdate("DELETE FROM collection_warc WHERE collection_id = :collectionId AND warc_id = :warcId")
	public abstract int deleteCollectionWarc(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId);

	@SqlUpdate("INSERT INTO collection_warc (collection_id, warc_id, records, record_bytes) VALUES (:collectionId, :warcId, :records, :recordBytes)")
	public abstract void insertCollectionWarc(@Bind("collectionId") long collectionId, @Bind("warcId") long warcId, @Bind("records") long records, @Bind("recordBytes") long recordBytes);

	public static class Crawl {
		public final long id;
		public final String name;
		public final Long totalDocs;
		public final Long totalBytes;
		public final Long crawlSeriesId;
		public final Path path;
		public final int state;
		public final long warcFiles;
		public final long warcSize;
		public final long records;
		public final long recordBytes;
		public final String description;
		public final Date startTime;
		public final Date endTime;

		public Crawl(ResultSet rs) throws SQLException {
			String path = rs.getString("path");
			Integer state = (Integer)rs.getObject("state");
			id = rs.getLong("id");
			name = rs.getString("name");
			totalDocs = (Long)rs.getObject("total_docs");
			totalBytes = (Long)rs.getObject("total_bytes");
			crawlSeriesId = (Long)rs.getObject("crawl_series_id");
			this.path = path != null ? Paths.get(path) : null;
			this.state = state != null ? state : 0;
			warcFiles = rs.getLong("warc_files");
			warcSize = rs.getLong("warc_size");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
			description = rs.getString("description");
			startTime = (Date)rs.getObject("start_time");
			endTime = (Date)rs.getObject("end_time");
		}

		private static final String[] STATE_NAMES = {"Archived", "Importing", "Import Failed"};

		public String stateName() {
			return STATE_NAMES[state];
		}
	}

	public static class CrawlWithSeriesName extends Crawl {
		public final String seriesName;


		public CrawlWithSeriesName(ResultSet rs) throws SQLException {
			super(rs);
			this.seriesName = rs.getString("crawl_series.name");
		}
	}

	public static class CrawlMapper implements ResultSetMapper<Crawl> {
		@Override
		public Crawl map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new Crawl(r);
		}
	}

	public static class CrawlWithSeriesNameMapper implements ResultSetMapper<CrawlWithSeriesName> {
		@Override
		public CrawlWithSeriesName map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new CrawlWithSeriesName(r);
		}
	}

	public static final int ARCHIVED = 0;
	public static final int IMPORTING = 1;
	public static final int IMPORT_FAILED = 2;

	@SqlUpdate("INSERT INTO crawl (name, crawl_series_id, state) VALUES (:name, :crawl_series_id, :state)")
	@GetGeneratedKeys
	public abstract long createCrawl(@Bind("name") String name, @Bind("crawl_series_id") Long crawlSeriesId, @Bind("state") int state);

	@SqlQuery("SELECT * FROM crawl WHERE id = :id")
	public abstract Crawl findCrawl(@Bind("id") long crawlId);

	@SqlQuery("SELECT * FROM crawl WHERE crawl_series_id = :crawl_series_id ORDER BY id DESC")
	public abstract Iterable<Crawl> findCrawlsByCrawlSeriesId(@Bind("crawl_series_id") long crawlSeriesId);

	@SqlQuery("SELECT * FROM crawl WHERE state = :state")
	public abstract List<Crawl> findCrawlsByState(@Bind("state") int state);

	@SqlUpdate("UPDATE crawl SET path = :path WHERE id = :id")
	public abstract int updateCrawlPath(@Bind("id") long id, @Bind("path") String path);

	@SqlUpdate("UPDATE crawl SET state = :state WHERE id = :crawlId")
	public abstract int updateCrawlState(@Bind("crawlId") long crawlId, @Bind("state") int state);

	@SqlUpdate("UPDATE crawl SET name = :name, description = :description WHERE id = :crawlId")
	public abstract int updateCrawl(@Bind("crawlId") long crawlId, @Bind("name") String name, @Bind("description") String description);

	@SqlUpdate("UPDATE crawl SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
	public abstract int incrementRecordStatsForCrawl(@Bind("id") long crawlId, @Bind("records") long records, @Bind("bytes") long bytes);

	@SqlUpdate("UPDATE crawl SET start_time = :time WHERE id = :crawlId AND (start_time IS NULL OR start_time > :time)")
	public abstract int conditionallyUpdateCrawlStartTime(@Bind("crawlId") long crawlId, @Bind("time") Date time);

	@SqlUpdate("UPDATE crawl SET end_time = :time WHERE id = :crawlId AND (end_time IS NULL OR end_time < :time)")
	public abstract int conditionallyUpdateCrawlEndTime(@Bind("crawlId") long crawlId, @Bind("time") Date time);

	@SqlQuery("SELECT crawl.*, crawl_series.name FROM crawl LEFT JOIN crawl_series ON crawl.crawl_series_id = crawl_series.id ORDER BY crawl.end_time DESC, crawl.id DESC LIMIT :limit OFFSET :offset")
	public abstract List<CrawlWithSeriesName> paginateCrawlsWithSeriesName(@Bind("limit") long limit, @Bind("offset") long offset);

	@SqlQuery("SELECT COUNT(*) FROM crawl")
	public abstract long countCrawls();

	@SqlUpdate("UPDATE crawl SET warc_files = (SELECT COALESCE(COUNT(*), 0) FROM warc WHERE warc.crawl_id = crawl.id), warc_size = (SELECT COALESCE(SUM(size), 0) FROM warc WHERE warc.crawl_id = crawl.id), records = (SELECT COALESCE(SUM(records), 0) FROM warc WHERE warc.crawl_id = crawl.id), record_bytes = (SELECT COALESCE(SUM(record_bytes), 0) FROM warc WHERE warc.crawl_id = crawl.id)")
	public abstract int refreshWarcStatsOnCrawls();

	public static class CrawlSeries {
		public final long id;
		public final String name;
		public final Path path;
		public final long warcFiles;
		public final long warcSize;
		public final long records;
		public final long recordBytes;
		public final String description;

		public CrawlSeries(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			name = rs.getString("name");
			path = Paths.get(rs.getString("path"));
			warcFiles = rs.getLong("warc_files");
			warcSize = rs.getLong("warc_size");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
			description = rs.getString("description");
		}
	}

	public static class CrawlSeriesWithCount extends CrawlSeries {

		public final long crawlCount;


		public CrawlSeriesWithCount(ResultSet rs) throws SQLException {
			super(rs);
			crawlCount = rs.getLong("crawl_count");
		}
	}

	public static class CrawlSeriesMapper implements ResultSetMapper<CrawlSeries> {
		@Override
		public CrawlSeries map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new CrawlSeries(r);
		}
	}

	public static class CrawlSeriesWithCountMapper implements ResultSetMapper<CrawlSeriesWithCount> {
		@Override
		public CrawlSeriesWithCount map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new CrawlSeriesWithCount(r);
		}
	}

	@SqlQuery("SELECT * FROM crawl_series WHERE id = :id")
	public abstract CrawlSeries findCrawlSeriesById(@Bind("id") long crawlSeriesId);

	@SqlQuery("SELECT * FROM crawl_series ORDER BY name")
	public abstract List<CrawlSeries> listCrawlSeries();

	@SqlQuery("SELECT COUNT(*) FROM crawl_series")
	public abstract long countCrawlSeries();

	@SqlQuery("SELECT *, (SELECT COUNT(*) FROM crawl WHERE crawl_series_id = crawl_series.id) crawl_count FROM crawl_series ORDER BY name LIMIT :limit OFFSET :offset")
	public abstract List<CrawlSeriesWithCount> paginateCrawlSeries(@Bind("limit") long limit, @Bind("offset") long offset);

	@SqlUpdate("UPDATE crawl_series SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
	public abstract int incrementRecordStatsForCrawlSeries(@Bind("id") long crawlSeriesId, @Bind("records") long records, @Bind("bytes") long bytes);

	@SqlUpdate("INSERT INTO crawl_series (name, path) VALUES (:name, :path)")
	@GetGeneratedKeys
	public abstract long createCrawlSeries(@Bind("name") String name, @Bind("path") String path);

	@SqlUpdate("UPDATE crawl_series SET name = :name, path = :path, description = :description WHERE id = :id")
	public abstract int updateCrawlSeries(@Bind("id") long seriesId, @Bind("name") String name, @Bind("path") String path, @Bind("description") String description);

	@Transaction
	public int updateCrawlSeries(long seriesId, String name, String path, String description, List<Long> collectionIds, List<String> collectionUrlFilters) {
		int rows = updateCrawlSeries(seriesId, name, path, description);
		if (rows > 0) {
			removeCrawlSeriesFromAllCollections(seriesId);
			addCrawlSeriesToCollections(seriesId, collectionIds, collectionUrlFilters);
		}
		return rows;
	}

	@SqlUpdate("UPDATE crawl_series SET warc_files = (SELECT COALESCE(SUM(warc_files), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), warc_size = (SELECT COALESCE(SUM(warc_size), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), records = (SELECT COALESCE(SUM(records), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), record_bytes = (SELECT COALESCE(SUM(record_bytes), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id)")
	public abstract int refreshWarcStatsOnCrawlSeries();

	public static class Seed {
		public final long id;
		public final String url;
		public final String surt;
		public final long seedlistId;

		public Seed(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			url = rs.getString("url");
			surt = rs.getString("surt");
			seedlistId = rs.getLong("seedlist_id");
		}

		public String topPrivateDomain() {
			try {
				return InternetDomainName.from(URLParser.parse(url).getHost()).topPrivateDomain().toString();
			} catch (URISyntaxException e) {
				return url;
			}
		}

		public String highlighted() {
			String domain = StringEscapeUtils.escapeHtml(topPrivateDomain());
			String pattern = "(" + Pattern.quote(domain) + ")([:/]|$)";
			return "<span class='hlurl'>" + url.replaceFirst(pattern, "<span class='domain'>$1</span>$2") + "</span>";
		}
	}

	public static class SeedMapper implements ResultSetMapper<Seed> {
		@Override
		public Seed map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
			return new Seed(resultSet);
		}
	}

	@SqlQuery("SELECT * FROM seed WHERE seedlist_id = :id ORDER BY surt")
	public abstract List<Seed> findSeedsBySeedListId(@Bind("id") long seedlistId);

	@SqlBatch("INSERT INTO seed (seedlist_id, url, surt) VALUES (:seedlistId, :urls, :surts)")
	public abstract void insertSeedsOnly(@Bind("seedlistId") long seedlistId, @Bind("urls") List<String> urls, @Bind("surts") List<String> surts);

	@SqlUpdate("DELETE FROM seed WHERE seedlist_id = :seedlistId")
	public abstract int deleteSeedsBySeedlistId(@Bind("seedlistId") long seedlistId);

	public static class Seedlist {
		public final long id;
		public final String name;
		public final String description;
		public final long totalSeeds;

		public Seedlist(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			name = rs.getString("name");
			description = rs.getString("description");
			totalSeeds = rs.getLong("total_seeds");
		}
	}

	public static class SeedlistMapper implements ResultSetMapper<Seedlist> {
		@Override
		public Seedlist map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
			return new Seedlist(resultSet);
		}
	}

	@SqlQuery("SELECT * FROM seedlist")
	public abstract List<Seedlist> listSeedlists();

	@SqlUpdate("INSERT INTO seedlist (name, description) VALUES (:name, :description)")
	@GetGeneratedKeys
	public abstract long createSeedlist(@Bind("name") String name, @Bind("description") String description);

	@SqlUpdate("UPDATE seedlist SET name = :name, description = :description WHERE id = :id")
	public abstract int updateSeedlist(@Bind("id") long id, @Bind("name") String name, @Bind("description") String description);

	@SqlUpdate("UPDATE seedlist SET name = :name, description = :description, total_seeds = :totalSeeds WHERE id = :id")
	public abstract int updateSeedlist(@Bind("id") long id, @Bind("name") String name, @Bind("description") String description, @Bind("totalSeeds") long totalSeeds);

	@SqlQuery("SELECT * FROM seedlist WHERE id = :id")
	public abstract Seedlist findSeedlist(@Bind("id") long id);

	@SqlUpdate("DELETE FROM seedlist WHERE id = :seedlistId")
	public abstract int deleteSeedlistOnly(@Bind("seedlistId") long seedlistId);

	@Transaction
	public int deleteSeedlist(long seedlistId) {
		deleteSeedsBySeedlistId(seedlistId);
		return deleteSeedlistOnly(seedlistId);
	}

	@SqlUpdate("UPDATE seedlist SET total_seeds = total_seeds + :delta WHERE id = :id")
	public abstract int incrementSeedlistTotalSeeds(@Bind("id") long id, @Bind("delta") long delta);

	@Transaction
	public void insertSeeds(long seedlistId, List<String> urls, List<String> surts) {
		insertSeedsOnly(seedlistId, urls, surts);
		incrementSeedlistTotalSeeds(seedlistId, urls.size());
	}

	@Transaction
	public void updateSeedlist(long seedlistId, String name, String description, List<String> urls, List<String> surts) {
		assert(urls.size() == surts.size());
		deleteSeedsBySeedlistId(seedlistId);
		insertSeedsOnly(seedlistId, urls, surts);
		updateSeedlist(seedlistId, name, description, urls.size());
	}

	public static class Warc {
		public final static int OPEN = 0, IMPORTED = 1, CDX_INDEXED = 2, SOLR_INDEXED = 3;
		public final static int IMPORT_ERROR = -1, CDX_ERROR = -2, SOLR_ERROR = -3;

		public final long id;
		public final long crawlId;
		public final int stateId;
		public final Path path;
		public final long size;
		public final long records;
		public final long recordBytes;
		public final String filename;
		public final String sha256;

		public Warc(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			crawlId = rs.getLong("crawl_id");
			stateId= rs.getInt("warc_state_id");
			path = Paths.get(rs.getString("path"));
			size = rs.getLong("size");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
			filename = rs.getString("filename");
			sha256 = rs.getString("sha256");
		}
	}

	public static class WarcMapper implements ResultSetMapper<Warc> {
		@Override
		public Warc map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
			return new Warc(resultSet);
		}
	}

	@Transaction
	public long insertWarc(long crawlId, int stateId, String path, String filename, long size, String sha256) {
		incrementWarcStatsForCrawl(crawlId, 1, size);
		incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 1, size);
		long warcId = insertWarcWithoutRollup(crawlId, stateId, path, filename, size, sha256);
		insertWarcHistory(warcId, stateId);
		return warcId;
	}

	@Transaction
	public int updateWarc(long crawlId, long warcId, int stateId, String path, String filename, long oldSize, long size, String sha256) {
		int rows = updateWarcWithoutRollup(warcId, stateId, path, filename, size, sha256);
		if (rows > 0) {
			insertWarcHistory(warcId, stateId);
			incrementWarcStatsForCrawl(crawlId, 0, size - oldSize);
			incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 0, size - oldSize);
		}
		return rows;
	}

	@Transaction
	public int updateWarcSize(long crawlId, long warcId, long oldSize, long size) {
		int rows = updateWarcSizeWithoutRollup(warcId, size);
		if (rows > 0) {
			incrementWarcStatsForCrawl(crawlId, 0, size - oldSize);
			incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 0, size - oldSize);
		}
		return rows;
	}

	@SqlQuery("SELECT crawl_id FROM warc WHERE id = :warcId")
	public abstract long findCrawlIdForWarc(@Bind("warcId") long warcId);

	@SqlUpdate("UPDATE warc SET warc_state_id = :stateId, path = :path, filename = :filename, size = :size, sha256 = :sha256 WHERE id = :warcId")
	public abstract int updateWarcWithoutRollup(@Bind("warcId") long warcId, @Bind("stateId") int stateId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size, @Bind("sha256") String sha256);

	@SqlUpdate("UPDATE crawl_series SET warc_files = warc_files + :warc_files,  warc_size = warc_size + :warc_size WHERE id = (SELECT crawl_series_id FROM crawl WHERE crawl.id = :crawl_id)")
	public abstract void incrementWarcStatsForCrawlSeriesByCrawlId(@Bind("crawl_id") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

	@SqlUpdate("UPDATE crawl SET warc_files = warc_files + :warc_files, warc_size = warc_size + :warc_size WHERE id = :crawlId")
	public abstract void incrementWarcStatsForCrawl(@Bind("crawlId") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

	@SqlUpdate("INSERT INTO warc (crawl_id, path, filename, size, warc_state_id, sha256) VALUES (:crawlId, :path, :filename, :size, :stateId, :sha256)")
	@GetGeneratedKeys
	public abstract long insertWarcWithoutRollup(@Bind("crawlId") long crawlId, @Bind("stateId") int stateId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size, @Bind("sha256") String sha256);

	@SqlQuery("SELECT * FROM warc")
	public abstract List<Warc> listWarcs();

	@SqlQuery("SELECT * FROM warc WHERE id = :warcId")
	public abstract Warc findWarc(@Bind("warcId") long warcId);

	@SqlQuery("SELECT * FROM warc WHERE filename = :filename")
	public abstract Warc findWarcByFilename(@Bind("filename") String filename);

	@SqlQuery("SELECT * FROM warc WHERE path = :path")
	public abstract Warc findWarcByPath(@Bind("path") String path);

	@SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId")
	public abstract List<Warc> findWarcsByCrawlId(@Bind("crawlId") long crawlId);

	@SqlQuery("SELECT * FROM warc WHERE warc_state_id = :stateId LIMIT :limit")
	public abstract List<Warc> findWarcsInState(@Bind("stateId") int stateId, int limit);

	@SqlQuery("SELECT COUNT(*) FROM warc WHERE warc_state_id = :stateId")
	public abstract long countWarcsInState(@Bind("stateId") int stateId);

	@SqlQuery("SELECT * FROM warc WHERE warc_state_id = :stateId LIMIT :limit OFFSET :offset")
	public abstract List<Warc> paginateWarcsInState(@Bind("stateId") int stateId, @Bind("limit") long limit, @Bind("offset") long offset);

	@SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId ORDER BY filename LIMIT :limit OFFSET :offset")
	public abstract List<Warc> paginateWarcsInCrawl(@Bind("crawlId") long crawlId, @Bind("limit") long limit, @Bind("offset") long offset);

	@SqlQuery("SELECT * FROM warc WHERE crawl_id = :crawlId AND warc_state_id = :stateId LIMIT :limit OFFSET :offset")
	public abstract List<Warc> paginateWarcsInCrawlAndState(@Bind("crawlId") long crawlId, @Bind("stateId") int stateId, @Bind("limit") long limit, @Bind("offset") long offset);

	@SqlQuery("SELECT COUNT(*) FROM warc WHERE crawl_id = :it AND warc_state_id = :stateId")
	public abstract long countWarcsInCrawlAndState(@Bind long crawlId,  @Bind("stateId") int stateId);

	@Transaction
	public int updateWarcState(long warcId, int stateId) {
		int rows = updateWarcStateWithoutHistory(warcId, stateId);
		if (rows > 0) {
			insertWarcHistory(warcId, stateId);
		}
		return rows;
	}

	@SqlUpdate("UPDATE warc SET warc_state_id = :stateId WHERE id = :warcId")
	public abstract int updateWarcStateWithoutHistory(@Bind("warcId") long warcId, @Bind("stateId") int stateId);

	@SqlUpdate("INSERT INTO warc_history (warc_id, warc_state_id) VALUES (:warcId, :stateId)")
	public abstract int insertWarcHistory(@Bind("warcId") long warcId, @Bind("stateId") int stateId);

	@SqlUpdate("UPDATE warc SET records = :records, record_bytes = :record_bytes WHERE id = :id")
	public abstract int updateWarcRecordStats(@Bind("id") long warcId, @Bind("records") long records, @Bind("record_bytes") long recordBytes);

	@SqlUpdate("UPDATE warc SET size = :size WHERE id = :id")
	public abstract int updateWarcSizeWithoutRollup(@Bind("id") long warcId, @Bind("size") long size);

	@SqlUpdate("UPDATE warc SET sha256 = :digest WHERE id = :id")
	public abstract int updateWarcSha256(@Bind("id") long id, @Bind("digest") String digest);

	@SqlQuery("SELECT name FROM warc_state WHERE id = :stateId")
	public abstract String findWarcStateName(@Bind("stateId") int stateId);
}
