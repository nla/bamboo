package bamboo.pandas;

import bamboo.core.Config;

import java.util.*;

public class PandasRestrictions {
    PandasDAO dao;

    public PandasRestrictions(PandasDAO dao) {
        this.dao = dao;
    }

    void dump() {
        for (PeriodRestr restr : dao.listPeriodRestrictions()) {
            Set<String> urls = urlsForTitle(restr.getTitleId());
            AccessRule rule = new AccessRule();
            rule.policy = "block";
            rule.surt = "au,gov,nla,pandora,)/pan/" + restr.getTitleId() + "/";
            rule.secondsSinceCapture = restr.getSecondsSinceCapture();
            rule.privateComment = "Migrated from PANDAS";
            rule.enabled = true;
            rule.who = "ip:" + Integer.toString(restr.getAgencyAreaId());

            System.out.println(rule.toString());
        }
    }

    private Set<String> urlsForTitle(long titleId) {
        return null;
    }

    static class AccessRule {
        Long id;
        String policy;
        String surt;
        Date captureStart;
        Date captureEnd;
        Date retrievalStart;
        Date retrievalEnd;
        Long secondsSinceCapture;
        String who;
        String privateComment;
        String publicComment;
        Boolean enabled;
        Boolean exactMatch = Boolean.FALSE;

        @Override
        public String toString() {
            return "AccessRule{" +
                    "id=" + id +
                    ", policy='" + policy + '\'' +
                    ", surt='" + surt + '\'' +
                    ", captureStart=" + captureStart +
                    ", captureEnd=" + captureEnd +
                    ", retrievalStart=" + retrievalStart +
                    ", retrievalEnd=" + retrievalEnd +
                    ", secondsSinceCapture=" + secondsSinceCapture +
                    ", who='" + who + '\'' +
                    ", privateComment='" + privateComment + '\'' +
                    ", publicComment='" + publicComment + '\'' +
                    ", enabled=" + enabled +
                    ", exactMatch=" + exactMatch +
                    '}';
        }
    }

    public static void main(String args[]) {
        PandasDB db = new PandasDB(new Config());
        new PandasRestrictions(db.dao).dump();
    }

}
