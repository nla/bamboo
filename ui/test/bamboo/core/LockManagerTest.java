package bamboo.core;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class LockManagerTest {
    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    @Test
    public void test() throws IOException {
        LockManagerDAO dao = fixtures.dbPool.dao().lockManager();
        try (LockManager lockManager = new LockManager(dao)) {
            assertEquals(true, lockManager.takeLock("warc-1"));
            assertEquals(false, lockManager.takeLock("warc-1"));
            lockManager.releaseLock("warc-1");
            assertEquals(true, lockManager.takeLock("warc-1"));
            lockManager.releaseLock("warc-1");

            // just for syntax checking
            dao.expireStaleLocks(lockManager.expiryTime);
        }
    }


}
