package bamboo.seedlist;

import bamboo.core.Fixtures;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SeedlistsTest {

    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    @Test
    public void testCRUD() {
        Seedlists seedlists = new Seedlists(fixtures.dao.seedlists());

        long id = seedlists.create(new Seedlists.Update() {
            @Override
            public String getName() {
                return "slname";
            }

            @Override
            public String getDescription() {
                return "sldesc";
            }

            @Override
            public Collection<Seed> getSeeds() {
                return Arrays.asList(new Seed("http://example.org/"));
            }
        });

        Seedlist seedlist = seedlists.get(id);
        assertEquals("slname", seedlist.getName());
        assertEquals("sldesc", seedlist.getDescription());
        assertEquals(1, seedlists.listSeeds(id).size());

        seedlists.delete(id);
        assertNull(seedlists.getOrNull(id));
    }


}
