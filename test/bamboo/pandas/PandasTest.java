package bamboo.pandas;

import bamboo.core.TestConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class PandasTest {

    @ClassRule
    public static TemporaryFolder tmp = new TemporaryFolder();
    private static Pandas pandas;

    @BeforeClass
    public static void setUp() throws IOException {
        Path warcDir = tmp.newFolder("pandas-warcs").toPath();
        TestConfig config = new TestConfig();
        config.setPandasWarcDir(warcDir);
        pandas = new Pandas(config, null, null);
        try (Handle handle = pandas.dbi.open()) {
            handle.execute("CREATE TABLE TITLE (AGENCY_ID NUMBER, ANBD_NUMBER VARCHAR2(22), " +
                    "AWAITING_CONFIRMATION NUMBER, CONTENT_WARNING VARCHAR2(256), CURRENT_OWNER_ID NUMBER, " +
                    "CURRENT_STATUS_ID NUMBER, DEFAULT_PERMISSION_ID NUMBER, DISAPPEARED NUMBER, FORMAT_ID NUMBER, " +
                    "INDEXER_ID NUMBER, IS_CATALOGUING_NOT_REQ NUMBER, IS_SUBSCRIPTION NUMBER, LEGACY_PURL VARCHAR2(1024), " +
                    "LOCAL_DATABASE_NO VARCHAR2(25), LOCAL_REFERENCE VARCHAR2(25), NAME VARCHAR2(256), NOTES VARCHAR2(4000), " +
                    "PERMISSION_ID NUMBER, PI NUMBER, PUBLISHER_ID NUMBER, REG_DATE TIMESTAMP (6), SEED_URL VARCHAR2(1024), " +
                    "SHORT_DISPLAY_NAME VARCHAR2(256), TEP_ID NUMBER, TITLE_ID NUMBER NOT NULL,TITLE_RESOURCE_ID NUMBER, " +
                    "STANDING_ID NUMBER, STATUS_ID NUMBER, TITLE_URL VARCHAR2(1024), UNABLE_TO_ARCHIVE NUMBER)");
            handle.execute("CREATE TABLE INSTANCE (" +
                    "CURRENT_STATE_ID NUMBER, " +
                    "DISPLAY_NOTE VARCHAR2(4000), " +
                    "GATHER_COMMAND VARCHAR2(4000), " +
                    "GATHER_METHOD_NAME VARCHAR2(256), " +
                    "GATHERED_URL VARCHAR2(1024), " +
                    "INSTANCE_DATE TIMESTAMP (6), " +
                    "INSTANCE_ID NUMBER NOT NULL," +
                    "INSTANCE_STATE_ID NUMBER, " +
                    "INSTANCE_STATUS_ID NUMBER, " +
                    "IS_DISPLAYED NUMBER, " +
                    "PREFIX VARCHAR2(256), " +
                    "PROCESSABLE NUMBER, " +
                    "REMOVEABLE NUMBER, " +
                    "RESOURCE_ID NUMBER, " +
                    "RESTRICTABLE NUMBER, " +
                    "RESTRICTION_ENABLED_T NUMBER, " +
                    "TEP_URL VARCHAR2(1024), " +
                    "TITLE_ID NUMBER, " +
                    "TRANSPORTABLE NUMBER, " +
                    "TYPE_NAME VARCHAR2(256)" +
                    ")");
            handle.execute("CREATE TABLE TITLE_GATHER " +
                    "(ACTIVE_PROFILE_ID NUMBER, " +
                    "ADDITIONAL_URLS VARCHAR2(4000), " +
                    "AUTHENTICATE_IP NUMBER, " +
                    "AUTHENTICATE_USER NUMBER, " +
                    "CAL_START_DATE TIMESTAMP (6), " +
                    "FIRST_GATHER_DATE TIMESTAMP (6), " +
                    "GATHER_METHOD_ID NUMBER(10,0), " +
                    "GATHER_SCHEDULE_ID NUMBER(10,0), " +
                    "GATHER_URL VARCHAR2(4000), " +
                    "LAST_GATHER_DATE TIMESTAMP (6), " +
                    "NEXT_GATHER_DATE TIMESTAMP (6), " +
                    "NOTES VARCHAR2(4000), " +
                    "PASSWORD VARCHAR2(128), " +
                    "QUEUED NUMBER, " +
                    "IS_SCHEDULED NUMBER, " +
                    "SCHEDULED_DATE TIMESTAMP (6), " +
                    "TITLE_ID NUMBER NOT NULL," +
                    "USERNAME VARCHAR2(128), " +
                    "GATHER_COMMAND VARCHAR2(4000)" +
                    ")");
            handle.execute("CREATE TABLE STATUS " +
                    "   (    STATUS_ID NUMBER NOT NULL, " +
                    "        STATUS_NAME VARCHAR2(128)" +
                    "   )");

            handle.execute("  CREATE TABLE INDIVIDUAL " +
                    "   (    AUDIT_CREATE_DATE TIMESTAMP (6), " +
                    "        AUDIT_CREATE_USERID NUMBER, " +
                    "        AUDIT_DATE TIMESTAMP (6), " +
                    "        AUDIT_USERID NUMBER, " +
                    "        COMMENTS VARCHAR2(100), " +
                    "        EMAIL VARCHAR2(120), " +
                    "        FAX VARCHAR2(25), " +
                    "        FUNCTION VARCHAR2(120), " +
                    "        INDIVIDUAL_ID NUMBER NOT NULL," +
                    "        IS_ACTIVE NUMBER, " +
                    "        MOBILE_PHONE VARCHAR2(25), " +
                    "        NAME_FAMILY VARCHAR2(30), " +
                    "        NAME_GIVEN VARCHAR2(130), " +
                    "        NAME_TITLE VARCHAR2(12), " +
                    "        PASSWORD VARCHAR2(100), " +
                    "        PHONE VARCHAR2(25), " +
                    "        URL VARCHAR2(1024), " +
                    "        USERID VARCHAR2(20)" +
                    "   ) ;");

            handle.execute("  CREATE TABLE ORGANISATION " +
                    "   (    ALIAS VARCHAR2(500), " +
                    "        AGENCY_ID NUMBER, " +
                    "        AUDIT_DATE TIMESTAMP (6), " +
                    "        AUDIT_USERID NUMBER, " +
                    "        COMMENTS VARCHAR2(100), " +
                    "        LONGCOUNTRY VARCHAR2(100), " +
                    "        EMAIL VARCHAR2(100), " +
                    "        FAX VARCHAR2(20), " +
                    "        INDEXER_ID NUMBER, " +
                    "        LINE1 VARCHAR2(200), " +
                    "        LINE2 VARCHAR2(200), " +
                    "        LOCALITY VARCHAR2(46), " +
                    "        MOBILE_PHONE VARCHAR2(20), " +
                    "        NAME VARCHAR2(256), " +
                    "        ORGANISATION_ID NUMBER NOT NULL," +
                    "        PHONE VARCHAR2(20), " +
                    "        POSTCODE VARCHAR2(10), " +
                    "        PUBLISHER_ID NUMBER, " +
                    "        SERVICE_ID NUMBER, " +
                    "        LONGSTATE VARCHAR2(100), " +
                    "        URL VARCHAR2(1024)" +
                    "   ) ;");
            handle.execute("  CREATE TABLE AGENCY " +
                    "   (    AGENCY_ID NUMBER NOT NULL," +
                    "        EXTERNAL_EMAIL VARCHAR2(64), " +
                    "        FORM_LETTER_URL VARCHAR2(4000), " +
                    "        LOCAL_DATABASE_PREFIX VARCHAR2(64), " +
                    "        LOCAL_REFERENCE_PREFIX VARCHAR2(64), " +
                    "        LOGO BLOB, " +
                    "        ORGANISATION_ID NUMBER" +
                    "   ) ;");

            handle.execute("INSERT INTO TITLE (PI, TITLE_ID, NAME) VALUES (?, ?, ?)", 1, 1, "test title");

        }
    }

    @Test
    public void test() throws IOException {
        List<PandasTitle> titles = new ArrayList<>();
        pandas.iterateTitles().forEachRemaining(titles::add);
        PandasTitle title = titles.get(0);
        assertTrue(title.name.equals("test title"));


    }
}
