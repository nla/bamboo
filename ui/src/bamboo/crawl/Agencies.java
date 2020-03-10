package bamboo.crawl;

import bamboo.core.NotFoundException;

public class Agencies {
    private final AgencyDAO dao;

    public Agencies(AgencyDAO dao) {
        this.dao = dao;
    }

    public Agency getOrNull(int id) {
        return dao.findAgencyById(id);
    }

    public Agency get(int id) {
        return NotFoundException.check(getOrNull(id), "agency", id);
    }
}
