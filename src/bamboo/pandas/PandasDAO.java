package bamboo.pandas;

import bamboo.core.Config;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.FetchSize;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;

@RegisterMapper({PandasDAO.TitleMapper.class})
interface PandasDAO extends Closeable {

    @SqlQuery("select GATHER_URL, PI, TITLE.NAME, STATUS_NAME, INDIVIDUAL.USERID as OWNER, ORGANISATION.ALIAS as AGENCY\n" +
            "from TITLE_GATHER\n" +
            "left join TITLE on TITLE_GATHER.TITLE_ID = TITLE.TITLE_ID\n" +
            "left join STATUS on STATUS.STATUS_ID = TITLE.CURRENT_STATUS_ID\n" +
            "left join INDIVIDUAL ON INDIVIDUAL_ID = CURRENT_OWNER_ID\n" +
            "left join AGENCY ON TITLE.AGENCY_ID = AGENCY.AGENCY_ID\n" +
            "left join ORGANISATION ON ORGANISATION.ORGANISATION_ID = AGENCY.ORGANISATION_ID\n" +
            "where NULLIF(GATHER_URL, '') IS NOT NULL")
    @FetchSize(500)
    ResultIterator<PandasTitle> iterateTitles();

    class TitleMapper implements ResultSetMapper<PandasTitle> {
        @Override
        public PandasTitle map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new PandasTitle(resultSet);
        }
    }

    class InstanceMapper implements ResultSetMapper<PandasInstance> {
        private final Config config;

        public InstanceMapper(Config config) {
            this.config = config;
        }

        @Override
        public PandasInstance map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
            return new PandasInstance(config.getPandasWarcDir(), resultSet);
        }
    }

    @SqlQuery("SELECT instance_id, pi, TO_CHAR(instance_date, 'YYYYMMDD-HH24MI') dt, name FROM instance, title WHERE instance.title_id = title.title_id AND instance_id = :instanceId")
    PandasInstance findInstance(@Bind("instanceId") long instanceId);

    void close();
}
