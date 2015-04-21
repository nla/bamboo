package bamboo.core;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public abstract class Db implements AutoCloseable, Transactional {

	public abstract  void close();

	@SqlUpdate("UPDATE crawl SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
	public abstract int incrementRecordStatsForCrawl(@Bind("id") long crawlId, @Bind("records") long records, @Bind("bytes") long bytes);

	@SqlUpdate("UPDATE crawl_series SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
	public abstract int incrementRecordStatsForCrawlSeries(@Bind("id") long crawlSeriesId, @Bind("records") long records, @Bind("bytes") long bytes);

	@SqlUpdate("UPDATE collection SET records = records + :records, record_bytes = record_bytes + :bytes WHERE id = :id")
	public abstract int incrementRecordStatsForCollection(@Bind("id") long collectionId, @Bind("records") long records, @Bind("bytes") long bytes);

	public static class Collection {
		public final long id;
		public final String name;
		public final String cdxUrl;
		public final String solrUrl;
		public final long records;
		public final long recordBytes;

		public Collection(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			name = rs.getString("name");
			cdxUrl = rs.getString("cdx_url");
			solrUrl = rs.getString("solr_url");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
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

	@SqlQuery("SELECT * FROM collection ORDER BY name")
	public abstract Iterable<Collection> listCollections();

	@SqlQuery("SELECT collection.*, collection_series.url_filters FROM collection_series LEFT JOIN collection ON collection.id = collection_id WHERE crawl_series_id = :it")
	public abstract List<CollectionWithFilters> listCollectionsForCrawlSeries(@Bind long crawlSeriesId);

	@SqlUpdate("DELETE FROM collection_series WHERE crawl_series_id = :it")
	public abstract void removeCrawlSeriesFromAllCollections(@Bind long crawlSeriesId);

	@SqlBatch("INSERT INTO collection_series (crawl_series_id, collection_id, url_filters) VALUES (:crawl_series_id, :collection_id, :url_filters)")
	public abstract void addCrawlSeriesToCollections(@Bind("crawl_series_id") long crawlSeriesId, @Bind("collection_id") List<Long> collectionIds, @Bind("url_filters") List<String> urlFilters);

	@SqlQuery("SELECT * FROM collection WHERE id = :id")
	public abstract  Collection findCollection(@Bind("id") long id);

	@SqlUpdate("UPDATE collection SET name = :name, cdx_url = :cdxUrl, solr_url = :solrUrl WHERE id = :id")
	public abstract int updateCollection(@Bind("id") long collectionId, @Bind("name")  String name, @Bind("cdxUrl") String cdxUrl, @Bind("solrUrl")  String solrUrl);

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
		}

		private static final String[] STATE_NAMES = {"Importing"};

		public String stateName() {
			return STATE_NAMES[state];
		}
	}

	public static class CrawlMapper implements ResultSetMapper<Crawl> {
		@Override
		public Crawl map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new Crawl(r);
		}
	}

	@SqlQuery("SELECT * FROM crawl")
	public abstract List<Crawl> listCrawls();

	@SqlUpdate("INSERT INTO crawl (name, crawl_series_id) VALUES (:name, :crawl_series_id)")
	@GetGeneratedKeys
	public abstract long createCrawl(@Bind("name") String name, @Bind("crawl_series_id") Long crawlSeriesId);

	@SqlQuery("SELECT * FROM crawl WHERE id = :id")
	public abstract Crawl findCrawl(@Bind("id") long crawlId);

	@SqlQuery("SELECT crawl.* FROM crawl LEFT JOIN cdx_crawl ON crawl.id = cdx_crawl.crawl_id WHERE cdx_id = :cdx_id")
	public abstract Iterable<Crawl> findCrawlsByCdxId(@Bind("cdx_id") long cdxId);

	@SqlQuery("SELECT * FROM crawl WHERE crawl_series_id = :crawl_series_id")
	public abstract Iterable<Crawl> findCrawlsByCrawlSeriesId(@Bind("crawl_series_id") long crawlSeriesId);

	@SqlUpdate("UPDATE crawl SET path = :path WHERE id = :id")
	public abstract int updateCrawlPath(@Bind("id") long id, @Bind("path") String path);

	@SqlQuery("SELECT * FROM crawl LIMIT :limit OFFSET :offset")
	public abstract List<Crawl> paginateCrawls(@Bind("limit") long limit, @Bind("offset") long offset);

	@SqlQuery("SELECT COUNT(*) FROM crawl")
	public abstract long countCrawls();

	@SqlUpdate("UPDATE crawl SET warc_files = (SELECT COALESCE(COUNT(*), 0) FROM warc WHERE warc.crawl_id = crawl.id), warc_size = (SELECT COALESCE(SUM(size), 0) FROM warc WHERE warc.crawl_id = crawl.id)")
	public abstract int refreshWarcStatsOnCrawls();

	public static class CrawlSeries {
		public final long id;
		public final String name;
		public final Path path;
		public final long warcFiles;
		public final long warcSize;
		public final long records;
		public final long recordBytes;

		public CrawlSeries(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			name = rs.getString("name");
			path = Paths.get(rs.getString("path"));
			warcFiles = rs.getLong("warc_files");
			warcSize = rs.getLong("warc_size");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
		}
	}

	public static class CrawlSeriesMapper implements ResultSetMapper<CrawlSeries> {
		@Override
		public CrawlSeries map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new CrawlSeries(r);
		}
	}

	@SqlQuery("SELECT * FROM crawl_series WHERE id = :id")
	public abstract CrawlSeries findCrawlSeriesById(@Bind("id") long crawlSeriesId);

	@SqlQuery("SELECT * FROM crawl_series ORDER BY name")
	public abstract List<CrawlSeries> listCrawlSeries();

	@SqlQuery("COUNT(*) FROM crawl_series")
	public abstract long countCrawlSeries();

	@SqlQuery("SELECT * FROM crawl_series ORDER BY name LIMIT :limit OFFSET :offset")
	public abstract List<CrawlSeries> paginateCrawlSeries(@Bind("limit") long limit, @Bind("offset") long offset);

	@SqlUpdate("INSERT INTO crawl_series (name, path) VALUES (:name, :path)")
	@GetGeneratedKeys
	public abstract long createCrawlSeries(@Bind("name") String name, @Bind("path") String path);

	@SqlUpdate("UPDATE crawl_series SET name = :name, path = :path WHERE id = :id")
	public abstract int updateCrawlSeries(@Bind("id") long seriesId, @Bind("name") String name, @Bind("path") String path);

	@Transaction
	public int updateCrawlSeries(long seriesId, String name, String path, List<Long> collectionIds, List<String> collectionUrlFilters) {
		int rows = updateCrawlSeries(seriesId, name, path);
		if (rows > 0) {
			removeCrawlSeriesFromAllCollections(seriesId);
			addCrawlSeriesToCollections(seriesId, collectionIds, collectionUrlFilters);
		}
		return rows;
	}

	@SqlUpdate("UPDATE crawl_series SET warc_files = (SELECT COALESCE(SUM(warc_files), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id), warc_size = (SELECT COALESCE(SUM(warc_size), 0) FROM crawl WHERE crawl.crawl_series_id = crawl_series.id)")
	public abstract int refreshWarcStatsOnCrawlSeries();

	public static class Warc {
		public final long id;
		public final long crawlId;
		public final Path path;
		public final long size;
		public final long cdxIndexed;
		public final long solrIndexed;
		public final long records;
		public final long recordBytes;
		public final String filename;

		public Warc(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			crawlId = rs.getLong("crawl_id");
			path = Paths.get(rs.getString("path"));
			size = rs.getLong("size");
			cdxIndexed = rs.getLong("cdx_indexed");
			solrIndexed = rs.getLong("solr_indexed");
			records = rs.getLong("records");
			recordBytes = rs.getLong("record_bytes");
			filename = rs.getString("filename");
		}
	}

	public static class WarcMapper implements ResultSetMapper<Warc> {
		@Override
		public Warc map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
			return new Warc(resultSet);
		}
	}

	@Transaction
	public long insertWarc(long crawlId, String path, String filename, long size) {
		incrementWarcStatsForCrawl(crawlId, 1, size);
		incrementWarcStatsForCrawlSeriesByCrawlId(crawlId, 1, size);
		return insertWarcWithoutRollup(crawlId, path, filename, size);
	}

	@SqlUpdate("UPDATE crawl_series SET warc_files = warc_files + :warc_files,  warc_size = warc_size + :warc_size WHERE id = (SELECT crawl_series_id FROM crawl WHERE crawl.id = :crawl_id)")
	public abstract void incrementWarcStatsForCrawlSeriesByCrawlId(@Bind("crawl_id") long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

	@SqlUpdate("UPDATE crawl SET warc_files = warc_files + :warc_files, warc_size = warc_size + :warc_size")
	public abstract void incrementWarcStatsForCrawl(long crawlId, @Bind("warc_files") int warcFilesDelta, @Bind("warc_size") long warcSizeDelta);

	@SqlUpdate("INSERT INTO warc (crawl_id, path, filename, size, cdx_indexed) VALUES (:crawlId, :path, :filename, :size, 0)")
	@GetGeneratedKeys
	public abstract long insertWarcWithoutRollup(@Bind("crawlId") long crawlId, @Bind("path") String path, @Bind("filename") String filename, @Bind("size") long size);

	@SqlQuery("SELECT * FROM warc")
	public abstract List<Warc> listWarcs();

	@SqlQuery("SELECT * FROM warc WHERE id = :warcId")
	public abstract Warc findWarc(@Bind("warcId") long warcId);

	@SqlQuery("SELECT * FROM warc WHERE filename = :filename")
	public abstract Warc findWarcByFilename(@Bind("filename") String filename);

	@SqlQuery("SELECT * FROM warc WHERE cdx_indexed = 0 AND corrupt = 0")
	public abstract List<Warc> findWarcsToCdxIndex();

    @SqlQuery("SELECT * FROM warc WHERE solr_indexed = 0 AND corrupt = 0")
	public abstract List<Warc> findWarcsToSolrIndex();

	@SqlUpdate("UPDATE warc SET cdx_indexed = :timestamp, records = :records, record_bytes = :record_bytes WHERE id = :id")
	public abstract int updateWarcCdxIndexed(@Bind("id") long warcId, @Bind("timestamp") long timestamp, @Bind("records") long records, @Bind("record_bytes") long recordBytes);

    @SqlUpdate("UPDATE warc SET solr_indexed = :timestamp WHERE id = :id")
	public abstract int updateWarcSolrIndexed(@Bind("id") long warcId, @Bind("timestamp") long timestamp);

	@SqlUpdate("UPDATE warc SET size = :size WHERE id = :id")
	public abstract int updateWarcSize(@Bind("id") long warcId, @Bind("size") long size);

	@SqlQuery("SELECT COUNT(*) FROM warc WHERE crawl_id = :it AND cdx_indexed = 0 AND corrupt = 0")
	public abstract long countWarcsToBeCdxIndexedInCrawl(@Bind long crawlId);

	@SqlQuery("SELECT COUNT(*) FROM warc WHERE crawl_id = :it AND solr_indexed = 0 AND corrupt = 0")
	public abstract long countWarcsToBeSolrIndexedInCrawl(@Bind long crawlId);

	@SqlQuery("SELECT COUNT(*) FROM warc WHERE crawl_id = :it AND corrupt <> 0")
	public abstract long countCorruptWarcsInCrawl(@Bind long crawlId);

	@SqlUpdate("UPDATE warc SET corrupt = :corrupt WHERE id = :id")
	public abstract int updateWarcCorrupt(@Bind("id") long warcId, @Bind("corrupt") int corrupt);

	public static final int GZIP_CORRUPT = 1;
	public static final int WARC_CORRUPT = 2;

}
