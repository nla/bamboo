package bamboo;

import bamboo.app.Bamboo;
import bamboo.core.TestConfig;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@TestConfiguration
public class AppTestConfig {
    @Bean
    public Bamboo getBamboo() throws IOException {
        return new Bamboo(new TestConfig(), false);
    }
}
