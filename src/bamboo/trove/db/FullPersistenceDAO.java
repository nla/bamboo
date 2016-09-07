package bamboo.trove.db;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface FullPersistenceDAO {
  public static final String ID_TABLE = "index_persistance_web_archives";
  public static final String ID_COLUMN = "last_warc_id";

  @SqlUpdate("UPDATE " + ID_TABLE + " SET " + ID_COLUMN + " = :lastId")
  public void updateLastId(@Bind("lastId") long lastId);

  @SqlQuery("SELECT " + ID_COLUMN + " FROM " + ID_TABLE)
  public long getLastId();
}