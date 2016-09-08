package bamboo.crawl;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CollectionWithFilters extends Collection {
    public final String urlFilters;

    public CollectionWithFilters(ResultSet rs) throws SQLException {
        super(rs);
        urlFilters = rs.getString("url_filters");
    }
}
