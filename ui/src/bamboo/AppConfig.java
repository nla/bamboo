package bamboo;

import bamboo.app.Bamboo;
import bamboo.core.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class AppConfig {
    @Bean
    public Bamboo getBamboo() throws IOException {
        return new Bamboo(new Config(System.getenv()), true);
    }
}
