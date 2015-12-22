package bamboo.seedlist;

import bamboo.core.Bamboo;
import bamboo.core.Db;
import droute.Request;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class Comparison {
    final Db.Seedlist seedlist1;
    final Db.Seedlist seedlist2;
    final List<Db.Seed> onlyIn1 = new ArrayList<>();
    final List<Db.Seed> onlyIn2 = new ArrayList<>();
    final List<Db.Seed> common = new ArrayList<>();

    Comparison(Bamboo bamboo, Request request) {
        List<Db.Seed> seeds1, seeds2;

        long id1 = Long.parseLong(request.urlParam("id1"));
        long id2 = Long.parseLong(request.urlParam("id2"));

        try (Db db = bamboo.dbPool.take()) {
            seedlist1 = db.findSeedlist(id1);
            seedlist2 = db.findSeedlist(id2);
            seeds1 = db.findSeedsBySeedListId(id1);
            seeds2 = db.findSeedsBySeedListId(id2);
        }

        Iterator<Db.Seed> it1 = seeds1.iterator();
        Iterator<Db.Seed> it2 = seeds2.iterator();

        Db.Seed seed1 = null, seed2 = null;

        for (;;) {
            if (seed1 == null && it1.hasNext()) {
                seed1 = it1.next();
            }
            if (seed2 == null && it2.hasNext()) {
                seed2 = it2.next();
            }

            int cmp;
            if (seed1 == null) {
                if (seed2 == null) {
                    break;
                }
                cmp = 1;
            } else if (seed2 == null) {
                cmp = -1;
            } else {
                cmp = seed1.surt.compareTo(seed2.surt);
            }

            if (cmp < 0) {
                onlyIn1.add(seed1);
                seed1 = null;
            } else if (cmp > 0) {
                onlyIn2.add(seed2);
                seed2 = null;
            } else {
                common.add(seed1);
                seed1 = null;
                seed2 = null;
            }
        }
    }

    public List<Db.Seed> sublist(String name) {
        switch (name) {
            case "onlyin1": return onlyIn1;
            case "onlyin2": return onlyIn2;
            case "common": return common;
            default: throw new IllegalArgumentException();
        }
    }
}
