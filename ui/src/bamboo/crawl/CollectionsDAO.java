package bamboo.crawl;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@RegisterRowMapper(CollectionsDAO.CollectionMapper.class)
public interface CollectionsDAO {

    class CollectionMapper implements RowMapper<Collection> {
        @Override
        public Collection map(ResultSet rs, StatementContext ctx) throws SQLException {
            return new Collection(rs);
        }
    }

    @SqlQuery("SELECT COUNT(*) FROM collection")
    long countCollections();

    @SqlQuery("SELECT * FROM collection ORDER BY name")
    List<Collection> listCollections();

    @SqlQuery("SELECT * FROM collection ORDER BY name LIMIT :limit OFFSET :offset")
    List<Collection> paginateCollections(@Bind("limit") long limit, @Bind("offset") long offset);

    @SqlQuery("SELECT collection.* FROM collection_series LEFT JOIN collection ON collection.id = collection_id WHERE crawl_series_id = :it")
    List<Collection> listCollectionsForCrawlSeries(@Bind("it") long crawlSeriesId);

    @SqlQuery("SELECT * FROM collection WHERE id = :id")
    Collection findCollection(@Bind("id") long id);

    @SqlUpdate("INSERT INTO collection(name, description, cdx_url) VALUES (:name, :description, :cdxUrl)")
    @GetGeneratedKeys
    long createCollection(@BindBean Collection collection);

    @SqlUpdate("UPDATE collection SET name = :coll.name, description = :coll.description, cdx_url = :coll.cdxUrl WHERE id = :id")
    int updateCollection(@Bind("id") long collectionId, @BindBean("coll") Collection coll);

}
