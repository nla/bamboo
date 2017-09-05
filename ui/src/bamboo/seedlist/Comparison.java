package bamboo.seedlist;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class Comparison {
    final Seedlist seedlist1;
    final Seedlist seedlist2;
    final List<Seed> onlyIn1 = new ArrayList<>();
    final List<Seed> onlyIn2 = new ArrayList<>();
    final List<Seed> common = new ArrayList<>();

    Comparison(Seedlists seedlists, long id1, long id2) {
        List<Seed> seeds1, seeds2;

        seedlist1 = seedlists.get(id1);
        seedlist2 = seedlists.get(id2);
        seeds1 = seedlists.listSeeds(id1);
        seeds2 = seedlists.listSeeds(id2);

        Iterator<Seed> it1 = seeds1.iterator();
        Iterator<Seed> it2 = seeds2.iterator();

        Seed seed1 = null, seed2 = null;

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
                cmp = seed1.getSurt().compareTo(seed2.getSurt());
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

    public List<Seed> sublist(String name) {
        switch (name) {
            case "onlyin1": return onlyIn1;
            case "onlyin2": return onlyIn2;
            case "common": return common;
            default: throw new IllegalArgumentException();
        }
    }
}
