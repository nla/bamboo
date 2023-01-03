package bamboo.crawl;

import bamboo.pandas.PandasAgency;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import java.util.List;

public interface AgencyDAO {
    @SqlQuery("SELECT * FROM agency WHERE id = :id")
    @RegisterBeanMapper(Agency.class)
    Agency findAgencyById(@Bind("id") int id);

    @SqlBatch("INSERT INTO agency (id, name, abbreviation, url, logo) " +
            "VALUES (:id, :name, :abbreviation, :url, :logo) " +
            "ON DUPLICATE KEY UPDATE name = :name, abbreviation = :abbreviation, url = :url, logo = :logo")
    void replaceAll(@BindBean List<PandasAgency> listAgencies);
}
