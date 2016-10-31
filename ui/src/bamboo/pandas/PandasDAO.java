package bamboo.pandas;

import bamboo.core.Config;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.FetchSize;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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

    @SqlQuery("select INSTANCE_ID from (select INSTANCE_ID from INSTANCE where CURRENT_STATE_ID = 1 and INSTANCE_ID > :startingFrom and TYPE_NAME <> 'Legacy pandora cgi' order by INSTANCE_ID asc) where rownum <= :limit")
    List<Long> listArchivedInstanceIds(@Bind("startingFrom") long startingFrom, @Bind("limit") int limit);

    @SqlQuery("select TITLE_ID titleId, PERIOD_MULTIPLIER periodMultiplier, PERIOD_TYPE_ID periodTypeId, AGENCY_AREA_ID agencyAreaId from PERIOD_RESTR")
    @MapResultAsBean
    List<PeriodRestr> listPeriodRestrictions();


    @SqlQuery("select INSTANCE_ID from (select INSTANCE_ID from INSTANCE where CURRENT_STATE_ID = 1 and INSTANCE_ID > :startingFrom and TYPE_NAME = :type order by INSTANCE_ID asc) where rownum <= :limit")
    List<Long> listArchivedInstanceIds(@Bind("type") String type, @Bind("startingFrom") long startingFrom, @Bind("limit") int limit);


    String SELECT_SUBJECT = "select SUBJECT.SUBJECT_ID id, SUBJECT_NAME name, SUBJECT_PARENT_ID parentId from SUBJECT ";

    @SqlQuery(SELECT_SUBJECT + "order by subject_parent_id asc")
    @MapResultAsBean
    List<PandasSubject> listSubjects();

    @SqlQuery(SELECT_SUBJECT + "left join COL_SUBS on SUBJECT.SUBJECT_ID = COL_SUBS.SUBJECT_ID where COL_ID = :collectionId")
    List<PandasSubject> listSubjectsForCollectionId(@Bind("collectionId") long collectionId);


    @SqlQuery("select COL_ID id, DISPLAY_COMMENT displayComment, DISPLAY_ORDER displayOrder, IS_DISPLAYED displayed, NAME name, COL_PARENT_ID parentId from COL")
    @MapResultAsBean
    List<PandasCollection> listCollections();

    @SqlQuery("select AGENCY_ID id, NAME name, LOGO logo, ORGANISATION.NAME name, ORGANISATION.URL url from agency left join organisation on ORGANISATION.ORGANISATION_ID = AGENCY.ORGANISATION_ID")
    @MapResultAsBean
    List<PandasAgency> listAgencies();


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

    @SqlQuery("SELECT instance_id, pi, TO_CHAR(instance_date, 'YYYYMMDD-HH24MI') dt, name FROM instance, title WHERE instance.title_id = :titleId AND title.title_id = :titleId")
    List<PandasInstance> listInstancesForTitle(@Bind("titleId") long titleId);

    void close();


}
