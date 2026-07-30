package bamboo.core;

import java.io.Closeable;
import java.lang.management.ManagementFactory;

/**
 * Implements a simple timeout-based distributed locking scheme.
 *
 * I'd like to just use MySQL's GET_LOCK, but it's too hard to make that work with connection pooling.
 */
public class LockManager implements Closeable {

    private final String myName = ManagementFactory.getRuntimeMXBean().getName();
    private final LockManagerDAO dao;
    private Thread keepaliveThread;
    private int keepaliveIntervalMillis = 5000;
    int expirySeconds = 120;
    private int expireStaleIntervalMillis = 300000;

    public LockManager(LockManagerDAO dao) {
        this.dao = dao;
    }

    public synchronized boolean takeLock(String lockName) {
        dao.expireLock(lockName, expirySeconds);
        int rows = dao.takeLock(lockName, myName, expirySeconds);
        if (rows > 0) {
            if (keepaliveThread == null) {
                keepaliveThread = new Thread(this::keepaliveLoop);
                keepaliveThread.setDaemon(true);
                keepaliveThread.setName("LockManager keepalive thread");
                keepaliveThread.start();
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean releaseLock(String lockName) {
        return dao.releaseLock(lockName, myName) > 0;
    }

    void keepaliveLoop() {
        long lastExpiryMillis = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                dao.checkin(myName);
                lastExpiryMillis = expireStaleLocksIfDue(System.currentTimeMillis(), lastExpiryMillis);
                Thread.sleep(keepaliveIntervalMillis);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    long expireStaleLocksIfDue(long nowMillis, long lastExpiryMillis) {
        if (nowMillis >= lastExpiryMillis + expireStaleIntervalMillis) {
            dao.expireStaleLocks(expirySeconds);
            return nowMillis;
        }
        return lastExpiryMillis;
    }

    @Override
    public synchronized void close() {
        if (keepaliveThread != null) {
            keepaliveThread.interrupt();
        }
    }
}
