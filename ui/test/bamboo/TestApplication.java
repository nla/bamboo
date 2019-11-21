package bamboo;

import bamboo.app.Bamboo;
import bamboo.core.TestConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;

@SpringBootApplication
@ComponentScan(excludeFilters = @ComponentScan.Filter(SpringBootApplication.class))
public class TestApplication {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean(destroyMethod = "close")
    public Bamboo getBamboo() throws IOException {
        return new Bamboo(new TestConfig(), false);
    }

}
