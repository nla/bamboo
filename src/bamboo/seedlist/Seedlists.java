package bamboo.seedlist;

import bamboo.core.Db;
import bamboo.core.DbPool;
import bamboo.core.NotFoundException;

import java.util.List;

public class Seedlists {
    private final DbPool dbPool;

    public Seedlists(DbPool dbPool) {
        this.dbPool = dbPool;
    }

    public Seedlist getOrNull(long id) {
        try (Db db = dbPool.take()) {
            return db.findSeedlist(id);
        }
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
        try (Db db = dbPool.take()) {
            return db.listSeedlists();
        }
    }

    public long create(Seedlist seedlist, List<Seed> seeds) {
        try (Db db = dbPool.take()) {
            long seedlistId = db.createSeedlist(seedlist);
            db.insertSeeds(seedlistId, seeds);
            return seedlistId;
        }
    }

    public List<Seed> listSeeds(long seedlistId) {
        try (Db db = dbPool.take()) {
            return db.findSeedsBySeedListId(seedlistId);
        }
    }

    public void update(long seedlistId, Seedlist seedlist, List<Seed> seeds) {
        try (Db db = dbPool.take()) {
            db.updateSeedlist(seedlistId, seedlist, seeds);
        }
    }

    public void delete(long seedlistId) {
        try (Db db = dbPool.take()) {
            db.deleteSeedlist(seedlistId);
        }
    }
}
