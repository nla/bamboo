package bamboo.crawl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

@RegisterMapper({CollectionsDAO.CollectionMapper.class, CollectionsDAO.CollectionWithFiltersMapper.class})
public interface CollectionsDAO {

    class CollectionMapper implements ResultSetMapper<Collection> {
        @Override
        public Collection map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
            return new Collection(rs);
        }
    }

    class CollectionWithFiltersMapper implements ResultSetMapper<CollectionWithFilters> {
        @Override
        public CollectionWithFilters map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
            return new CollectionWithFilters(rs);
        }
    }

    @SqlUpdate("SELECT COUNT(*) FROM collection")
    long countCollections();

    @SqlQuery("SELECT * FROM collection ORDER BY name")
    List<Collection> listCollections();

    @SqlQuery("SELECT * FROM collection ORDER BY name LIMIT :limit OFFSET :offset")
    List<Collection> paginateCollections(@Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT collection.*, collection_series.url_filters FROM collection_series LEFT JOIN collection ON collection.id = collection_id WHERE crawl_series_id = :it")
    List<CollectionWithFilters> listCollectionsForCrawlSeries(@Bind long crawlSeriesId);

    @SqlQuery("SELECT * FROM collection WHERE id = :id")
    Collection findCollection(@Bind("id") long id);

    @SqlUpdate("INSERT INTO collection(name, description, cdx_url, solr_url) VALUES (:name, :description, :cdxUrl, :solrUrl)")
    @GetGeneratedKeys
    long createCollection(@BindBean Collection collection);

    @SqlUpdate("UPDATE collection SET name = :coll.name, description = :coll.description, cdx_url = :coll.cdxUrl, solr_url = :coll.solrUrl WHERE id = :id")
    int updateCollection(@Bind("id") long collectionId, @BindBean("coll") Collection coll);

}
