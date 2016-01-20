package bamboo.seedlist;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

@RegisterMapper({SeedlistsDAO.SeedlistMapper.class, SeedlistsDAO.SeedMapper.class})
interface SeedlistsDAO extends Transactional<SeedlistsDAO> {
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

    @SqlUpdate("UPDATE seedlist SET name = :update.name, description = :update.description, total_seeds = :totalSeeds WHERE id = :id")
    int updateSeedlist(@Bind("id") long id, @BindBean("seedlist") Seedlists.Update update, @Bind("totalSeeds") long totalSeeds);

    @SqlQuery("SELECT * FROM seedlist WHERE id = :id")
    Seedlist findSeedlist(@Bind("id") long id);

    @SqlUpdate("DELETE FROM seedlist WHERE id = :seedlistId")
    int deleteSeedlistOnly(@Bind("seedlistId") long seedlistId);

    class SeedlistMapper implements ResultSetMapper<Seedlist> {
        @Override
        public Seedlist map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Seedlist(resultSet);
        }
    }

    class SeedMapper implements ResultSetMapper<Seed> {
        @Override
        public Seed map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Seed(resultSet);
        }
    }
}
