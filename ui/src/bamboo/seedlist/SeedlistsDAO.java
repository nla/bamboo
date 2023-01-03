package bamboo.seedlist;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

@RegisterRowMapper(SeedlistsDAO.SeedlistMapper.class)
@RegisterRowMapper(SeedlistsDAO.SeedMapper.class)
public interface SeedlistsDAO extends Transactional<SeedlistsDAO> {
    @SqlQuery("SELECT * FROM seed WHERE seedlist_id = :id ORDER BY surt")
    List<Seed> findSeedsBySeedListId(@Bind("id") long seedlistId);

    @SqlBatch("INSERT INTO seed (seedlist_id, url, surt) VALUES (:seedlistId, :seed.url, :seed.surt)")
    void insertSeedsOnly(@Bind("seedlistId") long seedlistId, @BindBean("seed") Collection<Seed> seeds);

    @SqlUpdate("DELETE FROM seed WHERE seedlist_id = :seedlistId")
    int deleteSeedsBySeedlistId(@Bind("seedlistId") long seedlistId);

    @SqlQuery("SELECT * FROM seedlist")
    List<Seedlist> listSeedlists();

    @SqlUpdate("INSERT INTO seedlist (name, description, total_seeds) VALUES (:update.name, :update.description, :totalSeeds)")
    @GetGeneratedKeys
    long insertSeedlist(@BindBean("update") Seedlists.Update update, @Bind("totalSeeds") int totalSeeds);

    @SqlUpdate("UPDATE seedlist SET name = :seedlist.name, description = :seedlist.description, total_seeds = :totalSeeds WHERE id = :id")
    int updateSeedlist(@Bind("id") long id, @BindBean("seedlist") Seedlists.Update update, @Bind("totalSeeds") long totalSeeds);

    @SqlQuery("SELECT * FROM seedlist WHERE id = :id")
    Seedlist findSeedlist(@Bind("id") long id);

    @SqlUpdate("DELETE FROM seedlist WHERE id = :seedlistId")
    int deleteSeedlistOnly(@Bind("seedlistId") long seedlistId);

    class SeedlistMapper implements RowMapper<Seedlist> {
        @Override
        public Seedlist map(ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Seedlist(resultSet);
        }
    }

    class SeedMapper implements RowMapper<Seed> {
        @Override
        public Seed map(ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Seed(resultSet);
        }
    }
}
