package bamboo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface Db extends AutoCloseable {

	public static class Cdx {
		public final long id;
		public final long collectionId;
		public final String path;
		public final long totalDocs;
		public final long totalBytes;

		public Cdx(long id, long collectionId, String path, long totalDocs, long totalBytes) {
			this.id = id;
			this.collectionId = collectionId;
			this.path = path;
			this.totalDocs = totalDocs;
			this.totalBytes = totalBytes;
		}
	}

	public static class CdxMapper implements ResultSetMapper<Cdx> {

		@Override
		public Cdx map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new Cdx(r.getLong("id"), r.getLong("collection_id"), r.getString("path"), r.getLong("total_docs"),
					r.getLong("total_bytes"));
		}
	}

	@SqlQuery("SELECT * FROM cdx WHERE collection_id = :collection_id")
	Iterable<Cdx> findCdxsByCollectionId(@Bind("collection_id") long collectionId);

	@SqlQuery("SELECT * FROM cdx WHERE id = :id")
	Cdx findCdx(@Bind("id") long id);

	public static class Collection {
		public final long id;
		public final String name;

		public Collection(long id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	public static class CollectionMapper implements ResultSetMapper<Collection> {
		@Override
		public Collection map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new Collection(r.getLong("id"), r.getString("name"));
		}
	}

	@SqlQuery("SELECT * FROM collection")
	Iterable<Collection> listCollections();

	@SqlQuery("SELECT * FROM collection WHERE id = :id")
	Collection findCollection(@Bind("id") long id);

	public static class Crawl {
		public final long id;
		public final String name;
		public final Long totalDocs;
		public final Long totalBytes;
		public final Long crawlSeriesId;

		public Crawl(long id, String name, Long totalDocs, Long totalBytes, Long crawlSeriesId) {
			this.id = id;
			this.name = name;
			this.totalDocs = totalDocs;
			this.totalBytes = totalBytes;
			this.crawlSeriesId = crawlSeriesId;
		}
	}

	public static class CrawlMapper implements ResultSetMapper<Crawl> {
		@Override
		public Crawl map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new Crawl(r.getLong("id"), r.getString("name"), (Long)r.getObject("total_docs"),
					(Long)r.getObject("total_bytes"), (Long)r.getObject("crawl_series_id"));
		}
	}
	@SqlQuery("SELECT crawl.* FROM crawl LEFT JOIN cdx_crawl ON crawl.id = cdx_crawl.crawl_id WHERE cdx_id = :cdx_id")
	Iterable<Crawl> findCrawlsByCdxId(@Bind("cdx_id") long cdxId);

	public static class CrawlSeries {
		public final long id;
		public final String name;
		public final Path path;

		public CrawlSeries(long id, String name, Path path) {
			this.id = id;
			this.name = name;
			this.path = path;
		}
	}

	public static class CrawlSeriesMapper implements ResultSetMapper<CrawlSeries> {
		@Override
		public CrawlSeries map(int index, ResultSet r, StatementContext ctx) throws SQLException {
			return new CrawlSeries(r.getLong("id"), r.getString("name"), Paths.get(r.getString("path")));
		}
	}

	@SqlQuery("SELECT * FROM crawl_series WHERE id = :id")
	CrawlSeries findCrawlSeriesById(@Bind("id") long crawlSeriesId);

	@SqlQuery("SELECT * FROM crawl_series ORDER BY name")
	List<CrawlSeries> listCrawlSeries();

	@SqlUpdate("INSERT INTO crawl_series (name, path) VALUES (:name, :path)")
	@GetGeneratedKeys
	long createCrawlSeries(@Bind("name") String name, @Bind("path") String path);

	/*
	 * TODO
	 */

	long addCrawl();

	void addWarc(long V, String s, String s1);

	void close();
}
