package bamboo.directory;

import bamboo.core.NotFoundException;

public class Agencies {
    private AgencyDAO dao;

    public Agency getByLegacyIdOrNull(int legacyTypeId, long legacyId) {
        return dao.findByLegacyId(legacyTypeId, legacyId);
    }

    public void save(Agency agency) {
        Long id = agency.getId();
        if (id == null) {
            agency.setId(dao.insert(agency));
        } else {
            int rows = dao.update(agency);
            if (rows == 0) {
                throw new NotFoundException("agency", id);
            }
        }
    }
}
