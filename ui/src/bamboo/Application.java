package bamboo;

import bamboo.app.Bamboo;
import bamboo.core.Config;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

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

        copyEnvToProperty("OIDC_URL", "spring.security.oauth2.client.provider.oidc.issuer-uri");
        copyEnvToProperty("OIDC_CLIENT_ID", "spring.security.oauth2.client.registration.oidc.client-id");
        copyEnvToProperty("OIDC_CLIENT_SECRET", "spring.security.oauth2.client.registration.oidc.client-secret");

        SpringApplication.run(Application.class, args);
    }

    private static void copyEnvToProperty(String env, String property) {
        String value = System.getenv(env);
        if (value != null && !value.isBlank()) {
            System.setProperty(property, value);
        }
    }

    @Bean
    public Bamboo getBamboo() throws IOException {
        return new Bamboo(new Config(System.getenv()), true);
    }
}
