package bamboo;

import bamboo.core.Config;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {
    public Config getConfig() {
        return new Config(System.getenv());
    }
}
