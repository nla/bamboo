package bamboo.core;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 * User-defined functions for H2 to make it behave more like MySQL.
 */
public class DbH2Compat {

    private DbH2Compat() {}

    /**
     * Create aliaes in a H2 database for the compatibility functions.
     */
    public static void register(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ALIAS IF NOT EXISTS SUBSTRING_INDEX DETERMINISTIC FOR \"" + DbH2Compat.class.getName() + ".substringIndex\"");
            stmt.execute("CREATE ALIAS IF NOT EXISTS FROM_UNIXTIME DETERMINISTIC FOR \"" + DbH2Compat.class.getName() + ".fromUnixtime\"");
            stmt.execute("CREATE ALIAS IF NOT EXISTS UNIX_TIMESTAMP FOR \"" + DbH2Compat.class.getName() + ".unixTimestamp\"");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Java implementation of MySQL's SUBSTRING_INDEX for use by H2.
     */
    public static String substringIndex(String str, String delim, int count) {
        if (str == null) {
            return null;
        }
        if (count > 0) {
            int pos = str.indexOf(delim);
            while (--count > 0 && pos != -1) {
                pos = str.indexOf(delim, pos + 1);
            }
            if (pos == -1) {
                return str;
            } else {
                return str.substring(0, pos);
            }
        } else if (count < 0) {
            int pos = str.lastIndexOf(delim);
            while (++count < 0 && pos != -1) {
                pos = str.lastIndexOf(delim, pos - 1);
            }
            if (pos == -1) {
                return str;
            } else {
                return str.substring(pos + 1);
            }
        } else {
            return "";
        }
    }

    /**
     * Java implementation of MySQL's FROM_UNIXTIME for use by H2.
     */
    public static Date fromUnixtime(long time) {
        return new Date(time);
    }

    public static long unixTimestamp() {
        return System.currentTimeMillis() / 1000;
    }
}
