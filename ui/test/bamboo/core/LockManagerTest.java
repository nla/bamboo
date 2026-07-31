package bamboo.core;

import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LockManagerTest {
    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    @Test
    public void takesAndReleasesLocks() {
        LockManagerDAO dao = fixtures.dbPool.dao().lockManager();
        try (LockManager lockManager = new LockManager(dao)) {
            assertEquals(true, lockManager.takeLock("warc-1"));
            assertEquals(false, lockManager.takeLock("warc-1"));
            lockManager.releaseLock("warc-1");
            assertEquals(true, lockManager.takeLock("warc-1"));
            lockManager.releaseLock("warc-1");
        }
    }

    @Test
    public void expiresStaleLocksOnSchedule() {
        RecordingLockManagerDAO dao = new RecordingLockManagerDAO();
        LockManager lockManager = new LockManager(dao);
        long firstExpiryMillis = 1_000_000;

        long lastExpiryMillis = lockManager.expireStaleLocksIfDue(firstExpiryMillis, 0);
        assertEquals(firstExpiryMillis, lastExpiryMillis);
        assertEquals(1, dao.expireStaleLocksCalls);

        lastExpiryMillis = lockManager.expireStaleLocksIfDue(firstExpiryMillis + 299_999, lastExpiryMillis);
        assertEquals(firstExpiryMillis, lastExpiryMillis);
        assertEquals(1, dao.expireStaleLocksCalls);

        lastExpiryMillis = lockManager.expireStaleLocksIfDue(firstExpiryMillis + 300_000, lastExpiryMillis);
        assertEquals(firstExpiryMillis + 300_000, lastExpiryMillis);
        assertEquals(2, dao.expireStaleLocksCalls);
        assertEquals(120, dao.lastExpirySeconds);
    }

    @Test
    public void databaseExpiryIsMeasuredInSeconds() {
        LockManagerDAO dao = fixtures.dbPool.dao().lockManager();

        dao.takeLock("recent-lock", "other-owner", 120);
        fixtures.dbPool.dbi.useHandle(handle -> handle.execute(
                "UPDATE named_lock SET checkin_time = UNIX_TIMESTAMP() - 60 WHERE name = ?", "recent-lock"));
        dao.expireLock("recent-lock", 120);
        assertEquals(1, dao.releaseLock("recent-lock", "other-owner"));

        dao.takeLock("stale-lock", "other-owner", 120);
        fixtures.dbPool.dbi.useHandle(handle -> handle.execute(
                "UPDATE named_lock SET checkin_time = UNIX_TIMESTAMP() - 121 WHERE name = ?", "stale-lock"));
        dao.expireLock("stale-lock", 120);
        assertEquals(0, dao.releaseLock("stale-lock", "other-owner"));
    }

    private static class RecordingLockManagerDAO implements LockManagerDAO {
        int expireStaleLocksCalls;
        long lastExpirySeconds;

        @Override
        public int checkin(String owner) {
            return 0;
        }

        @Override
        public int takeLock(String name, String owner, long expirySeconds) {
            return 0;
        }

        @Override
        public int releaseLock(String name, String owner) {
            return 0;
        }

        @Override
        public void expireStaleLocks(long expirySeconds) {
            expireStaleLocksCalls++;
            lastExpirySeconds = expirySeconds;
        }

        @Override
        public void expireLock(String name, long expirySeconds) {
        }
    }

}
