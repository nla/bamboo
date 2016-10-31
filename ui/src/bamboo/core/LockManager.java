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
    private int keepaliveInterval = 5000;
    int expiryTime = 120000;
    private int expireStaleInterval = 300000;

    public LockManager(LockManagerDAO dao) {
        this.dao = dao;
    }

    public synchronized boolean takeLock(String lockName) {
        dao.expireLock(lockName, expiryTime);
        int rows = dao.takeLock(lockName, myName, expiryTime);
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
        while (!Thread.currentThread().isInterrupted()) {
            long lastExpiry = 0;
            try {
                dao.checkin(myName);
                if (System.currentTimeMillis() < lastExpiry + expireStaleInterval) {
                    dao.expireStaleLocks(expiryTime);
                }
                Thread.sleep(keepaliveInterval);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public synchronized void close() {
        if (keepaliveThread != null) {
            keepaliveThread.interrupt();
        }
    }
}
