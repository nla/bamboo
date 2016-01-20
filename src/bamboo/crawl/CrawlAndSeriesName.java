package bamboo.crawl;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CrawlAndSeriesName extends Crawl {
    public final String seriesName;


    public CrawlAndSeriesName(ResultSet rs) throws SQLException {
        super(rs);
        this.seriesName = rs.getString("crawl_series.name");
    }
}
