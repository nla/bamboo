package bamboo.pandas;

import bamboo.core.Fixtures;
import bamboo.core.TestConfig;
import org.archive.util.SURT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;

public class PandasRestrictionsTest {
    @ClassRule
    public static Fixtures fixtures = new Fixtures();
    private static PandasDB db;
    private static PandasRestrictions pandasRestrictions;

    @BeforeClass
    public static void setUp() throws IOException {
        db = new PandasDB(new TestConfig() {
            @Override
            public String getPandasDbUrl() {
                return super.getPandasDbUrl() + "-restrictions";
            }
        });
        pandasRestrictions = new PandasRestrictions(db.dao);

        try (Handle h = db.dbi.open()) {
            h.execute("CREATE TABLE INSTANCE\n" +
                    "(\n" +
                    "    CURRENT_STATE_ID NUMBER,\n" +
                    "    DISPLAY_NOTE VARCHAR2(4000),\n" +
                    "    GATHER_COMMAND VARCHAR2(4000),\n" +
                    "    GATHER_METHOD_NAME VARCHAR2(256),\n" +
                    "    GATHERED_URL VARCHAR2(1024),\n" +
                    "    INSTANCE_DATE TIMESTAMP,\n" +
                    "    INSTANCE_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    INSTANCE_STATE_ID NUMBER,\n" +
                    "    INSTANCE_STATUS_ID NUMBER,\n" +
                    "    IS_DISPLAYED NUMBER,\n" +
                    "    PREFIX VARCHAR2(256),\n" +
                    "    PROCESSABLE NUMBER,\n" +
                    "    REMOVEABLE NUMBER,\n" +
                    "    RESOURCE_ID NUMBER,\n" +
                    "    RESTRICTABLE NUMBER,\n" +
                    "    RESTRICTION_ENABLED_T NUMBER,\n" +
                    "    TEP_URL VARCHAR2(1024),\n" +
                    "    TITLE_ID NUMBER,\n" +
                    "    TRANSPORTABLE NUMBER,\n" +
                    "    TYPE_NAME VARCHAR2(256)\n" +
                    ");");

            h.execute("CREATE TABLE ORGANISATION\n" +
                    "(\n" +
                    "    ALIAS VARCHAR2(500),\n" +
                    "    AGENCY_ID NUMBER,\n" +
                    "    AUDIT_DATE TIMESTAMP,\n" +
                    "    AUDIT_USERID NUMBER,\n" +
                    "    COMMENTS VARCHAR2(100),\n" +
                    "    LONGCOUNTRY VARCHAR2(100),\n" +
                    "    EMAIL VARCHAR2(100),\n" +
                    "    FAX VARCHAR2(20),\n" +
                    "    INDEXER_ID NUMBER,\n" +
                    "    LINE1 VARCHAR2(200),\n" +
                    "    LINE2 VARCHAR2(200),\n" +
                    "    LOCALITY VARCHAR2(46),\n" +
                    "    MOBILE_PHONE VARCHAR2(20),\n" +
                    "    NAME VARCHAR2(256),\n" +
                    "    ORGANISATION_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    PHONE VARCHAR2(20),\n" +
                    "    POSTCODE VARCHAR2(10),\n" +
                    "    PUBLISHER_ID NUMBER,\n" +
                    "    SERVICE_ID NUMBER,\n" +
                    "    LONGSTATE VARCHAR2(100),\n" +
                    "    URL VARCHAR2(1024),\n" +
                    ")");
            h.execute("CREATE TABLE AGENCY\n" +
                    "(\n" +
                    "    AGENCY_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    EXTERNAL_EMAIL VARCHAR2(64),\n" +
                    "    FORM_LETTER_URL VARCHAR2(4000),\n" +
                    "    LOCAL_DATABASE_PREFIX VARCHAR2(64),\n" +
                    "    LOCAL_REFERENCE_PREFIX VARCHAR2(64),\n" +
                    "    LOGO BLOB,\n" +
                    "    ORGANISATION_ID NUMBER,\n" +
                    "    CONSTRAINT AGENCY_ORGANISATION_FK FOREIGN KEY (ORGANISATION_ID) REFERENCES ORGANISATION (ORGANISATION_ID)\n" +
                    ");");
            h.execute("CREATE TABLE CONDITION\n" +
                    "(\n" +
                    "    CONDITION_DESCRIPTION VARCHAR2(4000),\n" +
                    "    CONDITION_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    NAME VARCHAR2(1024) NOT NULL\n" +
                    ")");
            h.execute("CREATE TABLE AGENCY_AREA\n" +
                    "(\n" +
                    "    AGENCY_AREA_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    AGENCY_ID NUMBER,\n" +
                    "    AREA_NAME VARCHAR2(256),\n" +
                    "    AREA_WORDING VARCHAR2(2048),\n" +
                    "    CONSTRAINT AGENCY_AREA_AGENCY_FK FOREIGN KEY (AGENCY_ID) REFERENCES AGENCY (AGENCY_ID)\n" +
                    ")");
            h.execute("CREATE TABLE PERIOD_TYPE\n" +
                    "(\n" +
                    "    PERIOD_TYPE VARCHAR2(20),\n" +
                    "    PERIOD_TYPE_DESCRIPTION VARCHAR2(1024),\n" +
                    "    PERIOD_TYPE_ID NUMBER PRIMARY KEY NOT NULL\n" +
                    ")");
            h.execute("CREATE TABLE PERIOD_RESTR\n" +
                    "(\n" +
                    "    AGENCY_AREA_ID NUMBER,\n" +
                    "    CONDITION_DATE TIMESTAMP,\n" +
                    "    CONDITION_ID NUMBER,\n" +
                    "    PERIOD_MULTIPLIER NUMBER,\n" +
                    "    PERIOD_RESTRICTION_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    PERIOD_TYPE_ID NUMBER,\n" +
                    "    TITLE_ID NUMBER,\n" +
                    ")");
            h.execute("CREATE TABLE TITLE\n" +
                    "(\n" +
                    "    AGENCY_ID NUMBER,\n" +
                    "    ANBD_NUMBER VARCHAR2(22),\n" +
                    "    AWAITING_CONFIRMATION NUMBER,\n" +
                    "    CONTENT_WARNING VARCHAR2(256),\n" +
                    "    CURRENT_OWNER_ID NUMBER,\n" +
                    "    CURRENT_STATUS_ID NUMBER,\n" +
                    "    DEFAULT_PERMISSION_ID NUMBER,\n" +
                    "    DISAPPEARED NUMBER,\n" +
                    "    FORMAT_ID NUMBER,\n" +
                    "    INDEXER_ID NUMBER,\n" +
                    "    IS_CATALOGUING_NOT_REQ NUMBER,\n" +
                    "    IS_SUBSCRIPTION NUMBER,\n" +
                    "    LEGACY_PURL VARCHAR2(1024),\n" +
                    "    LOCAL_DATABASE_NO VARCHAR2(25),\n" +
                    "    LOCAL_REFERENCE VARCHAR2(25),\n" +
                    "    NAME VARCHAR2(256),\n" +
                    "    NOTES VARCHAR2(4000),\n" +
                    "    PERMISSION_ID NUMBER,\n" +
                    "    PI NUMBER,\n" +
                    "    PUBLISHER_ID NUMBER,\n" +
                    "    REG_DATE TIMESTAMP(6),\n" +
                    "    SEED_URL VARCHAR2(1024),\n" +
                    "    SHORT_DISPLAY_NAME VARCHAR2(256),\n" +
                    "    TEP_ID NUMBER,\n" +
                    "    TITLE_ID NUMBER PRIMARY KEY NOT NULL,\n" +
                    "    TITLE_RESOURCE_ID NUMBER,\n" +
                    "    STANDING_ID NUMBER,\n" +
                    "    STATUS_ID NUMBER,\n" +
                    "    TITLE_URL VARCHAR2(1024),\n" +
                    "    UNABLE_TO_ARCHIVE NUMBER,\n" +
                    "    LEGAL_DEPOSIT NUMBER DEFAULT 0,\n" +
                    ")");

            h.execute("INSERT INTO PERIOD_RESTR (AGENCY_AREA_ID, CONDITION_DATE, CONDITION_ID, PERIOD_MULTIPLIER, PERIOD_RESTRICTION_ID, PERIOD_TYPE_ID, TITLE_ID) VALUES (3, DATEADD('MONTH', 1, CURRENT_TIMESTAMP), 1, 3, 641, 4, 103683)");
            h.execute("INSERT INTO INSTANCE (CURRENT_STATE_ID, DISPLAY_NOTE, GATHER_COMMAND, GATHER_METHOD_NAME, GATHERED_URL, INSTANCE_DATE, INSTANCE_ID, INSTANCE_STATE_ID, INSTANCE_STATUS_ID, IS_DISPLAYED, PREFIX, PROCESSABLE, REMOVEABLE, RESOURCE_ID, RESTRICTABLE, RESTRICTION_ENABLED_T, TEP_URL, TITLE_ID, TRANSPORTABLE, TYPE_NAME) VALUES (1, null, null, 'Upload', 'http://www.vcjournal.com.au/index.php?section=2', DATE '2009-08-06', 81761, null, null, 0, 'pan', 1, 1, null, 1, null, '/pan/103683/20090806-1232/APEVCJ Apr09.pdf', 103683, 1, 'Pandas 3')");
            h.execute("INSERT INTO INSTANCE (CURRENT_STATE_ID, DISPLAY_NOTE, GATHER_COMMAND, GATHER_METHOD_NAME, GATHERED_URL, INSTANCE_DATE, INSTANCE_ID, INSTANCE_STATE_ID, INSTANCE_STATUS_ID, IS_DISPLAYED, PREFIX, PROCESSABLE, REMOVEABLE, RESOURCE_ID, RESTRICTABLE, RESTRICTION_ENABLED_T, TEP_URL, TITLE_ID, TRANSPORTABLE, TYPE_NAME) VALUES (6, null, null, 'Upload', 'http://www.vcjournal.com.au/index.php?section=2', DATE '2013-06-21', 169891, null, null, 1, null, null, null, null, null, null, '/pan/103683/20130621-0000/www.vcjournal.com.au/index.php?section=2', 103683, null, 'Pandas 3')");
            h.execute("INSERT INTO INSTANCE (CURRENT_STATE_ID, DISPLAY_NOTE, GATHER_COMMAND, GATHER_METHOD_NAME, GATHERED_URL, INSTANCE_DATE, INSTANCE_ID, INSTANCE_STATE_ID, INSTANCE_STATUS_ID, IS_DISPLAYED, PREFIX, PROCESSABLE, REMOVEABLE, RESOURCE_ID, RESTRICTABLE, RESTRICTION_ENABLED_T, TEP_URL, TITLE_ID, TRANSPORTABLE, TYPE_NAME) VALUES (6, null, null, 'Upload', 'http://www.vcjournal.com.au/index.php?section=2', DATE '2015-03-04', 205018, null, null, 1, null, null, null, null, null, null, '/pan/103683/20150304-0915/March_2015.pdf', 103683, null, 'Pandas 3')");
            h.execute("INSERT INTO INSTANCE (CURRENT_STATE_ID, DISPLAY_NOTE, GATHER_COMMAND, GATHER_METHOD_NAME, GATHERED_URL, INSTANCE_DATE, INSTANCE_ID, INSTANCE_STATE_ID, INSTANCE_STATUS_ID, IS_DISPLAYED, PREFIX, PROCESSABLE, REMOVEABLE, RESOURCE_ID, RESTRICTABLE, RESTRICTION_ENABLED_T, TEP_URL, TITLE_ID, TRANSPORTABLE, TYPE_NAME) VALUES (1, null, null, 'Upload', 'http://www.vcjournal.com.au/index.php?section=2', DATE '2016-06-16', 235372, null, null, 0, 'pan', 1, 1, null, 1, null, '/pan/103683/20160616-0953/APE&VCJ_June%202016.pdf', 103683, 1, 'Pandas 3')");
            h.execute("INSERT INTO TITLE (AGENCY_ID, ANBD_NUMBER, AWAITING_CONFIRMATION, CONTENT_WARNING, CURRENT_OWNER_ID, CURRENT_STATUS_ID, DEFAULT_PERMISSION_ID, DISAPPEARED, FORMAT_ID, INDEXER_ID, IS_CATALOGUING_NOT_REQ, IS_SUBSCRIPTION, LEGACY_PURL, LOCAL_DATABASE_NO, LOCAL_REFERENCE, NAME, NOTES, PERMISSION_ID, PI, PUBLISHER_ID, REG_DATE, SEED_URL, SHORT_DISPLAY_NAME, TEP_ID, TITLE_ID, TITLE_RESOURCE_ID, STANDING_ID, STATUS_ID, TITLE_URL, UNABLE_TO_ARCHIVE, LEGAL_DEPOSIT) VALUES (1, '4666677', 0, null, 78489, 7, 76603, 0, 1, 6, 0, 0, null, null, 'NLA03/98', 'Australian Private Equity & Venture Capital Journal', 'Previously titled Australian venture capital journal. 5/8/09 YZ\n" +
                    "Issues received via email. 2/12/14 LM', 76603, 103683, 2628, TIMESTAMP '2009-08-05 15:42:39', 'http://www.vcjournal.com.au/index.php?section=2', 'Australian Private Equity & Venture Capital Journal', 41482, 103683, null, null, null, 'http://www.vcjournal.com.au/index.php?section=2', 0, 0)");

        }
    }

    @AfterClass
    public static void tearDown() {
        db.close();
    }

    @Test
    public void test() {
        for (PeriodRestr restr : db.dao.listPeriodRestrictions()) {
            for (PandasInstance instance : db.dao.listInstancesForTitle(103683)) {
                String surt = SURT.fromPlain(instance.panBaseUrl());
                System.out.println(surt + " " + restr.getSecondsSinceCapture());
            }
        }
    }

}
