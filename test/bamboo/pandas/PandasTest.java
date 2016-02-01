package bamboo.pandas;

import bamboo.core.Config;
import bamboo.core.Fixtures;
import bamboo.core.TestConfig;
import bamboo.crawl.Crawls;
import bamboo.crawl.Series;
import bamboo.crawl.Serieses;
import bamboo.crawl.Warcs;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PandasTest {

    @ClassRule
    public static TemporaryFolder tmp = new TemporaryFolder();

    @ClassRule
    public static Fixtures fixtures = new Fixtures();

    private static Pandas pandas;
    private static Warcs warcs;
    private static Serieses serieses;
    private static Crawls crawls;

    @BeforeClass
    public static void setUp() throws IOException {

        Path warcDir = tmp.newFolder("pandas-warcs").toPath();
        TestConfig config = new TestConfig();
        config.setPandasWarcDir(warcDir);


        warcs = new Warcs(fixtures.dao.warcs());
        serieses = new Serieses(fixtures.dao.serieses());
        crawls = new Crawls(fixtures.dao.crawls(), serieses, warcs);
        pandas = new Pandas(config, crawls, null);

        try (Handle db = pandas.dbi.open()) {
            /**
             * Create just the tables we use from PANDAS.  We don't bother with constraints as we never write to it.
             */

            db.execute("CREATE TABLE TITLE (AGENCY_ID NUMBER, ANBD_NUMBER VARCHAR2(22), " +
                    "AWAITING_CONFIRMATION NUMBER, CONTENT_WARNING VARCHAR2(256), CURRENT_OWNER_ID NUMBER, " +
                    "CURRENT_STATUS_ID NUMBER, DEFAULT_PERMISSION_ID NUMBER, DISAPPEARED NUMBER, FORMAT_ID NUMBER, " +
                    "INDEXER_ID NUMBER, IS_CATALOGUING_NOT_REQ NUMBER, IS_SUBSCRIPTION NUMBER, LEGACY_PURL VARCHAR2(1024), " +
                    "LOCAL_DATABASE_NO VARCHAR2(25), LOCAL_REFERENCE VARCHAR2(25), NAME VARCHAR2(256), NOTES VARCHAR2(4000), " +
                    "PERMISSION_ID NUMBER, PI NUMBER, PUBLISHER_ID NUMBER, REG_DATE TIMESTAMP (6), SEED_URL VARCHAR2(1024), " +
                    "SHORT_DISPLAY_NAME VARCHAR2(256), TEP_ID NUMBER, TITLE_ID NUMBER NOT NULL,TITLE_RESOURCE_ID NUMBER, " +
                    "STANDING_ID NUMBER, STATUS_ID NUMBER, TITLE_URL VARCHAR2(1024), UNABLE_TO_ARCHIVE NUMBER)");
            db.execute("CREATE TABLE INSTANCE (" +
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
            db.execute("CREATE TABLE TITLE_GATHER " +
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
            db.execute("CREATE TABLE STATUS " +
                    "   (    STATUS_ID NUMBER NOT NULL, " +
                    "        STATUS_NAME VARCHAR2(128)" +
                    "   )");

            db.execute("  CREATE TABLE INDIVIDUAL " +
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

            db.execute("  CREATE TABLE ORGANISATION " +
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
            db.execute("  CREATE TABLE AGENCY " +
                    "   (    AGENCY_ID NUMBER NOT NULL," +
                    "        EXTERNAL_EMAIL VARCHAR2(64), " +
                    "        FORM_LETTER_URL VARCHAR2(4000), " +
                    "        LOCAL_DATABASE_PREFIX VARCHAR2(64), " +
                    "        LOCAL_REFERENCE_PREFIX VARCHAR2(64), " +
                    "        LOGO BLOB, " +
                    "        ORGANISATION_ID NUMBER" +
                    "   ) ;");

            db.execute("INSERT INTO STATUS (STATUS_ID, STATUS_NAME) VALUES (?, ?)", 1, "archived");
            db.execute("INSERT INTO AGENCY (AGENCY_ID, ORGANISATION_ID) VALUES (?, ?)", 1, 1);
            db.execute("INSERT INTO ORGANISATION (ORGANISATION_ID, AGENCY_ID) VALUES (?, ?)", 1, 1);
            db.execute("INSERT INTO INDIVIDUAL (INDIVIDUAL_ID, USERID) VALUES (?, ?)", 1, "batman");

            db.execute("INSERT INTO TITLE (PI, TITLE_ID, NAME, AGENCY_ID, CURRENT_STATUS_ID) VALUES (?, ?, ?, ?, ?)", 10001, 1, "test title", 1, 1);
            db.execute("INSERT INTO TITLE_GATHER (title_id, gather_url) VALUES (?, ?)", 1, "http://example.org/");

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
            Date date = new Date();
            db.execute("INSERT INTO INSTANCE (INSTANCE_ID, TITLE_ID, CURRENT_STATE_ID, INSTANCE_DATE) VALUES (?, ?, ?, ?)", 42, 1, 1, date);
            Files.createDirectories(warcDir.resolve("010/10001"));
            Files.createFile(warcDir.resolve("010/10001/nla.arc-10001-" + dateFormat.format(date)));

            db.execute("INSERT INTO TITLE (PI, TITLE_ID, NAME, AGENCY_ID, CURRENT_STATUS_ID) VALUES (?, ?, ?, ?, ?)", 10002, 2, "test title2", 1, 1);
            db.execute("INSERT INTO TITLE_GATHER (title_id, gather_url) VALUES (?, ?)", 2, "http://two.example.org/");

        }
    }

    @Test
    public void testIterateTitles() throws IOException {
        List<PandasTitle> titles = new ArrayList<>();
        pandas.iterateTitles().forEachRemaining(titles::add);
        assertEquals(2, titles.size());
        assertTrue(titles.get(0).name.equals("test title"));
    }

    @Test
    public void testImport() throws IOException {
        Series series = new Series();
        series.setName("test pandas series");
        series.setPath(Paths.get("/tmp/invalid"));
        long seriesId = serieses.create(series);
        pandas.importAllInstances(seriesId);
        pandas.importAllInstances(seriesId);
    }

}
