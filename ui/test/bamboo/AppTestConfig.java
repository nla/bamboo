package bamboo;

import bamboo.core.Config;
import bamboo.core.TestConfig;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class AppTestConfig {
    @Bean
    public Config getConfig() {
        return new TestConfig();
    }
}
