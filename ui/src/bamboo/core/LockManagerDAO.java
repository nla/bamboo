package bamboo.core;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface LockManagerDAO {
    @SqlUpdate("UPDATE named_lock SET checkin_time = UNIX_TIMESTAMP() WHERE owner = :owner")
    int checkin(@Bind("owner") String owner);

    @SqlUpdate("INSERT INTO named_lock (name, owner, checkin_time, acquire_time) SELECT :name, :owner, UNIX_TIMESTAMP(), UNIX_TIMESTAMP() FROM DUAL WHERE NOT EXISTS (SELECT * FROM named_lock WHERE name = :name)")
    int takeLock(@Bind("name") String name, @Bind("owner") String owner,
                 @Bind("expirySeconds") long expirySeconds);

    @SqlUpdate("DELETE FROM named_lock WHERE name = :name AND owner = :owner")
    int releaseLock(@Bind("name") String name, @Bind("owner") String owner);

    @SqlUpdate("DELETE FROM named_lock WHERE checkin_time + :expirySeconds < UNIX_TIMESTAMP()")
    void expireStaleLocks(@Bind("expirySeconds") long expirySeconds);

    @SqlUpdate("DELETE FROM named_lock WHERE name = :name AND checkin_time + :expirySeconds < UNIX_TIMESTAMP()")
    void expireLock(@Bind("name") String name, @Bind("expirySeconds") long expirySeconds);
}
