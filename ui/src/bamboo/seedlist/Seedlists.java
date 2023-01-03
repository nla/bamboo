package bamboo.seedlist;

import bamboo.core.NotFoundException;

import java.util.Collection;
import java.util.List;

public class Seedlists {

    private final SeedlistsDAO dao;

    public Seedlists(SeedlistsDAO dao) {
        this.dao = dao;
    }

    public Seedlist getOrNull(long id) {
        return dao.findSeedlist(id);
    }

    /**
     * Retrieve a series's metadata.
     *
     * @throws NotFoundException if the crawl doesn't exist
     */
    public Seedlist get(long id) {
        return NotFoundException.check(getOrNull(id), "seedlist", id);
    }

    public List<Seedlist> listAll() {
        return dao.listSeedlists();
    }

    public long create(Update update) {
        return dao.inTransaction(tx -> {
            Collection<Seed> seeds = update.getSeeds();
            long seedlistId = tx.insertSeedlist(update, seeds.size());
            tx.insertSeedsOnly(seedlistId, seeds);
            return seedlistId;
        });
    }

    public List<Seed> listSeeds(long seedlistId) {
        return dao.findSeedsBySeedListId(seedlistId);
    }

    public void update(long seedlistId, Update update) {
        dao.inTransaction(tx -> {
            int rows = tx.updateSeedlist(seedlistId, update, update.getSeeds().size());
            if (rows == 0) {
                throw new NotFoundException("seedlist", seedlistId);
            }
            tx.deleteSeedsBySeedlistId(seedlistId);
            tx.insertSeedsOnly(seedlistId, update.getSeeds());
            return null;
        });
    }

    public void delete(long seedlistId) {
        dao.inTransaction(tx -> {
            tx.deleteSeedsBySeedlistId(seedlistId);
            int rows = tx.deleteSeedlistOnly(seedlistId);
            if (rows == 0) {
                throw new NotFoundException("seedlist", seedlistId);
            }
            return null;
        });
    }

    public interface Update {
        String getName();

        String getDescription();

        Collection<Seed> getSeeds();
    }

}
