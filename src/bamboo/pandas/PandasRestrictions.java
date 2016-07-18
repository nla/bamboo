package bamboo.pandas;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PandasRestrictions {
    PandasDAO dao;

    public PandasRestrictions(PandasDAO dao) {
        this.dao = dao;
    }

    void sync() {
        for (PeriodRestr restr : dao.listPeriodRestrictions()) {
            Set<String> urls = urlsForTitle(restr.getTitleId());
            System.out.println(restr.getPeriodMultiplier() + " * " + restr.getPeriodTypeId() + " " + urls);
        }
    }
}
