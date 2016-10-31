package bamboo.directory;

import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;

public interface AgencyDAO {
    @SqlQuery("SELECT id, name, logo, url, legacy_type_id legacyTypeId, legacy_id legacyId FROM dir_agency WHERE legacy_type_id = :legacyTypeId AND legacy_id = legacyId")
    @MapResultAsBean
    Agency findByLegacyId(@Bind("legacyTypeId") int legacyTypeId, @Bind("legacyId") long legacyId);

    @SqlUpdate("INSERT INTO agency (id, name, logo, :url, legacy_type_id, legacy_id) VALUES :id, :name, :logo, :url, :legacyTypeId, :legacyId")
    @GetGeneratedKeys
    long insert(@BindBean Agency agency);

    @SqlUpdate("UPDATE agency SET name = :name, logo = :logo, url = :url, legacy_type_id = :legacy_type_id, legacy_id = :legacy_id WHERE id = :id")
    int update(@BindBean Agency agency);
}
