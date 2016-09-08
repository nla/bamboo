package bamboo.trove.db;

import org.skife.jdbi.v2.sqlobject.mixins.Transactional;

public interface RestrictionsDAO extends Transactional<RestrictionsDAO> {
  // See examples like WarcsDAO for how mappers work if using POJOs
}
