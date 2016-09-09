package bamboo.pandas;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PandasTitle {
    public final String gatherUrl;
    public final long pi;
    public final String name;
    public final String status;
    public final String owner;
    public final String agency;

    PandasTitle(ResultSet rs) throws SQLException {
        gatherUrl = rs.getString("GATHER_URL");
        pi = rs.getLong("PI");
        name = rs.getString("NAME");
        status = rs.getString("STATUS_NAME");
        owner = rs.getString("OWNER");
        agency = rs.getString("AGENCY");
    }

    @Override
    public String toString() {
        return "Title{" +
                "gatherUrl='" + gatherUrl + '\'' +
                ", pi=" + pi +
                ", name='" + name + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
