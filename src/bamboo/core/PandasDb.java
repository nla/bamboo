package bamboo.core;

import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.FetchSize;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface PandasDb extends Closeable {

    @SqlQuery("select GATHER_URL, PI, TITLE.NAME, STATUS_NAME, INDIVIDUAL.USERID as OWNER, ORGANISATION.ALIAS as AGENCY\n" +
            "from TITLE_GATHER\n" +
            "left join TITLE on TITLE_GATHER.TITLE_ID = TITLE.TITLE_ID\n" +
            "left join STATUS on STATUS.STATUS_ID = TITLE.CURRENT_STATUS_ID\n" +
            "left join INDIVIDUAL ON INDIVIDUAL_ID = CURRENT_OWNER_ID\n" +
            "left join AGENCY ON TITLE.AGENCY_ID = AGENCY.AGENCY_ID\n" +
            "left join ORGANISATION ON ORGANISATION.ORGANISATION_ID = AGENCY.ORGANISATION_ID\n" +
            "where NULLIF(GATHER_URL, '') IS NOT NULL")
    @FetchSize(500)
    ResultIterator<Title> iterateTitles();

    class Title {
        public final String gatherUrl;
        public final long pi;
        public final String name;
        public final String status;
        public final String owner;
        public final String agency;

        Title(ResultSet rs) throws SQLException {
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

    class TitleMapper implements ResultSetMapper<Title> {
        @Override
        public Title map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new Title(resultSet);
        }
    }

    void close();
}
