package bamboo;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        if (System.getProperty("spring.security.oauth2.client.provider.oidc.issuer-uri") != null) {
            http.oauth2Login();
            http.logout().logoutSuccessUrl("/");
            http.authorizeRequests()
                    // api: solr indexer
                    .antMatchers("/collections/*/warcs/json").permitAll()
                    .antMatchers("/collections/*/warcs/sync").permitAll()
                    .antMatchers("/warcs/*/text").permitAll()
                    // api: wayback
                    .antMatchers("/warcs/*").permitAll()
                    .anyRequest().authenticated();
        }
    }
}
