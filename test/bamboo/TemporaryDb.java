package bamboo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.sql.DataSource;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TemporaryDb {
    private static Map<String, TemporaryDb> instances = new HashMap<>();
    private final HikariDataSource dataSource;
    private final String dbName;

    /**
     * Retrieves a datasource for a temporary database. The database will be create automatically and
     * then closed and dropped on JVM shutdown.
     */
    public static DataSource dataSource(String basename) throws SQLException {
        return singleton(basename).dataSource;
    }

    private synchronized static TemporaryDb singleton(String basename) throws SQLException {
        TemporaryDb instance = instances.get(basename);
        if (instance == null) {
            instance = new TemporaryDb(basename);
            instances.put(basename, instance);
        }
        return instance;
    }

    private TemporaryDb(String basename) throws SQLException {
        String baseJdbcUrl = envOrDefault("TEST_DB_URL", "jdbc:mysql://localhost");
        String user = envOrDefault("TEST_DB_USER", "root");
        String password = envOrDefault("TEST_DB_PASSWORD", "");

        this.dbName = generateName(basename);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName(dbName);
        hikariConfig.setJdbcUrl(baseJdbcUrl + "/" + dbName + "?createDatabaseIfNotExist=true");
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    private static String envOrDefault(String varName, String defaultValue) {
        String value = System.getenv(varName);
        return value != null ? value : defaultValue;
    }

    private String generateName(String basename) {
        SecureRandom random = new SecureRandom();
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        return basename + "_junit_" + df.format(new Date()) + "_" + random.nextInt(Integer.MAX_VALUE);
    }

    private void shutdown() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("DROP DATABASE " + dbName)) {
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        dataSource.close();
    }

    public static void main(String args[]) throws SQLException {
        try (Handle db = new DBI(TemporaryDb.dataSource("pizzadb")).open()) {
            db.execute("CREATE TABLE pizza(name VARCHAR(100))");
            db.execute("INSERT INTO pizza(name) VALUES (?)", "supreme");
            db.execute("INSERT INTO pizza(name) VALUES (?)", "vegetarian");
            System.out.println(db.createQuery("SELECT name FROM pizza").list());

            throw new RuntimeException("oh noes! make sure we still cleanup after an exception");
        }
    }
}
