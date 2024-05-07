package bamboo.config;

import org.springframework.boot.autoconfigure.orm.jpa.EntityManagerFactoryDependsOnPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.support.DatabaseStartupValidator;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

@Configuration
public class DatabaseStartupConfig {
    @Bean
    public DatabaseStartupValidator databaseStartupValidator(DataSource dataSource) {
        var validator = new DatabaseStartupValidator();
        validator.setDataSource(dataSource);
        validator.setTimeout((int) TimeUnit.DAYS.toSeconds(7));
        return validator;
    }

    /**
     * This makes EntityManagerFactory depend on DatabaseStartupValidator to block startup
     * until the database is ready.
     */
    @Bean
    public static EntityManagerFactoryDependsOnPostProcessor databaseStartupDependency() {
        return new EntityManagerFactoryDependsOnPostProcessor("databaseStartupValidator");
    }
}
