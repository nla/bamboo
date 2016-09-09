package bamboo.trove.db;

import org.skife.jdbi.v2.sqlobject.CreateSqlObject;

public interface TroveDaoRegistry {
  @CreateSqlObject
  public FullPersistenceDAO fullPersistence();

  @CreateSqlObject
  public RestrictionsDAO restrictions();
}