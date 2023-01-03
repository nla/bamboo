package bamboo;

import bamboo.app.Bamboo;
import bamboo.core.Config;
import bamboo.crawl.Crawl;
import bamboo.crawl.Series;
import bamboo.crawl.Warc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.rest.core.config.RepositoryRestConfiguration;
import org.springframework.data.rest.webmvc.config.RepositoryRestConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;

import javax.sql.DataSource;
import java.io.IOException;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h":
                case "--help":
                    System.out.println("Options:\n" +
                            "  -p port  Web server port\n" +
                            "  -b addr  Web server bind address\n" +
                            "  -c path  Web server context path");
                    return;
                case "-c":
                    System.setProperty("server.servlet.context-path", args[++i]);
                    break;
                case "-p":
                    System.setProperty("server.port", args[++i]);
                    break;
                case "-b":
                    System.setProperty("server.address", args[++i]);
                    break;
                default:
                    System.err.println("Ignoring unknown option: " + args[i]);
                    break;
            }
        }

        if (System.getenv("LOGIN_OIDC_URL") != null) {
            copyEnvToProperty("LOGIN_OIDC_URL", "spring.security.oauth2.client.provider.oidc.issuer-uri");
            copyEnvToProperty("LOGIN_OIDC_CLIENT_ID", "spring.security.oauth2.client.registration.oidc.client-id");
            copyEnvToProperty("LOGIN_OIDC_CLIENT_SECRET", "spring.security.oauth2.client.registration.oidc.client-secret");
        } else {
            copyEnvToProperty("OIDC_URL", "spring.security.oauth2.client.provider.oidc.issuer-uri");
            copyEnvToProperty("OIDC_CLIENT_ID", "spring.security.oauth2.client.registration.oidc.client-id");
            copyEnvToProperty("OIDC_CLIENT_SECRET", "spring.security.oauth2.client.registration.oidc.client-secret");
        }

        if (!System.getProperty("spring.security.oauth2.client.provider.oidc.issuer-uri", "").isEmpty()) {
            System.setProperty("spring.security.oauth2.client.registration.oidc.scope", "openid");
        }


        SpringApplication.run(Application.class, args);
    }

    private static void copyEnvToProperty(String env, String property) {
        String value = System.getenv(env);
        if (value != null && !value.isBlank()) {
            System.setProperty(property, value);
        }
    }

    @Bean(destroyMethod = "close")
    public Bamboo getBamboo() throws IOException {
        return new Bamboo(new Config(System.getenv()), true);
    }

    @Bean
    public DataSource getDataSource(@Autowired Bamboo bamboo) {
        return bamboo.getDataSource();
    }

    @Bean
    public RepositoryRestConfigurer repositoryRestConfigurer() {
        return new RepositoryRestConfigurer() {
            @Override
            public void configureRepositoryRestConfiguration(RepositoryRestConfiguration config, CorsRegistry cors) {
                config.exposeIdsFor(Crawl.class, Series.class, Warc.class);
            }
        };
    }
}
