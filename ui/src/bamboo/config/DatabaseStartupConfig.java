package bamboo.config;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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
    public static BeanFactoryPostProcessor entityManagerFactoryDependsOnPostProcessor() {
        return new BeanFactoryPostProcessor() {
            @Override
            public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
                String[] beanNames = beanFactory.getBeanNamesForType(EntityManagerFactory.class);
                for (String beanName : beanNames) {
                    BeanDefinition definition = beanFactory.getBeanDefinition(beanName);
                    definition.setDependsOn("databaseStartupValidator");
                }
            }
        };
    }
}
