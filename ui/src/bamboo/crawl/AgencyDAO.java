package bamboo.crawl;

import bamboo.pandas.PandasAgency;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;

import java.util.List;

public interface AgencyDAO {
    @SqlQuery("SELECT * FROM agency WHERE id = :id")
    @MapResultAsBean
    Agency findAgencyById(@Bind("id") int id);

    @SqlBatch("INSERT INTO agency (id, name, abbreviation, url, logo) " +
            "VALUES (:id, :name, :abbreviation, :url, :logo) " +
            "ON DUPLICATE KEY UPDATE name = :name, abbreviation = :abbreviation, url = :url, logo = :logo")
    void replaceAll(@BindBean List<PandasAgency> listAgencies);
}
