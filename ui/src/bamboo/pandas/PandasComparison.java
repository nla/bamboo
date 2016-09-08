package bamboo.pandas;

import bamboo.seedlist.Seed;
import bamboo.seedlist.Seedlist;
import bamboo.seedlist.Seedlists;
import org.archive.url.WaybackURLKeyMaker;
import org.skife.jdbi.v2.ResultIterator;

import java.net.URISyntaxException;
import java.util.*;

public class PandasComparison {
    private final static WaybackURLKeyMaker keyMaker = new WaybackURLKeyMaker();

    public final Map<String, Seed> seeds;
    public final Set<Seed> unmatched = new HashSet<>();
    public final List<Match> matches = new ArrayList<>();

    PandasComparison(PandasDAO dao, Seedlists seedlists, long seedlistId) {
        /*
         * We need to match the normalized version of all seed urls against the
         * PANDAS database. Since we don't store normalized URLs in either
         * database we have to fetch both lists and compare them.
         */

        /*
         * So first let's put all the seeds into a hashmap keyed by normalized url.
         */

        seeds = seedsByCanonicalizedUrl(seedlists, seedlistId);
        unmatched.addAll(seeds.values());

        /*
         * Now stream all the urls in the PANDAS database and look them up
         * one by one.  This whole procedure will need revisiting if PANDAS
         * gets a lot more titles but for now it's acceptable (< 2 seconds)
         * and doesn't require any caching or schema changes.
         */
        try (ResultIterator<PandasTitle> it = dao.iterateTitles()) {
            while (it.hasNext()) {
                PandasTitle title = it.next();
                if (title.gatherUrl == null) {
                    continue;
                }
                String url = title.gatherUrl.trim();
                if (url.isEmpty()) {
                    continue;
                }
                String key;
                try {
                    key = keyMaker.makeKey(title.gatherUrl);
                } catch (URISyntaxException e) {
                    continue;
                }
                Seed seed = seeds.get(key);
                if (seed != null) {
                    matches.add(new Match(seed, title));
                    unmatched.remove(seed);
                }
            }
        }

        matches.sort((a, b) -> a.seed.getSurt().compareTo(b.seed.getSurt()));
        List<Seed> unmatchedSorted = new ArrayList<>(unmatched);
        unmatchedSorted.sort((a, b) -> a.getSurt().compareTo(b.getSurt()));
    }

    Map<String, Seed> seedsByCanonicalizedUrl(Seedlists seedlists, long seedlistId) {
        Map<String, Seed> seeds = new HashMap<>();
        for (Seed seed : seedlists.listSeeds(seedlistId)) {
            try {
                seeds.put(keyMaker.makeKey(seed.getUrl()), seed);
            } catch (URISyntaxException e) {
                // skip it
            }
        }
        return seeds;
    }


    public static class Match {
        public final Seed seed;
        public final PandasTitle title;

        public Match(Seed seed, PandasTitle title) {
            this.seed = seed;
            this.title = title;
        }
    }
}
