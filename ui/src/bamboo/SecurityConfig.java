package bamboo;

import com.nimbusds.jwt.SignedJWT;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserRequest;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserService;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;

import java.text.ParseException;
import java.util.*;

@Configuration
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);
    private static final String[] ROLE_NAMES = new String[] {"stduser", "agadmin", "panadmin", "sysadmin"};

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        if (System.getProperty("spring.security.oauth2.client.provider.oidc.issuer-uri") != null) {
            http.oauth2Login().userInfoEndpoint().oidcUserService(oidcUserService());
            http.logout().logoutSuccessUrl("/");
            http.authorizeRequests()
                    // static content
                    .antMatchers("/webjars/**").permitAll()
                    .antMatchers("/assets/**").permitAll()

                    // api: solr indexer
                    .antMatchers("/collections/*/warcs/json").permitAll()
                    .antMatchers("/collections/*/warcs/sync").permitAll()
                    .antMatchers("/warcs/*/text").permitAll()
                    // api: wayback
                    .antMatchers("/warcs/*").permitAll()

                    .antMatchers("/").hasRole("stduser")
                    .antMatchers("/series").hasRole("stduser")
                    .antMatchers("/series/*").hasRole("stduser")

                    .anyRequest().hasRole("panadmin");
        }
    }

    private OAuth2UserService<OidcUserRequest, OidcUser> oidcUserService() {
        OidcUserService delegate = new OidcUserService();
        return (userRequest) -> {
            OidcUser oidcUser = delegate.loadUser(userRequest);
            String accessTokenValue = userRequest.getAccessToken().getTokenValue();
            Set<GrantedAuthority> mappedAuthorities = new HashSet<>(oidcUser.getAuthorities());
            try {
                JSONObject decoded = SignedJWT.parse(accessTokenValue).getPayload().toJSONObject();
                JSONArray array = (JSONArray)(((JSONObject)decoded.get("realm_access")).get("roles"));
                HashSet<Object> set = new HashSet<>(array);
                for (String roleName: ROLE_NAMES) {
                    if (set.contains(roleName)) {
                        mappedAuthorities.add(new SimpleGrantedAuthority("ROLE_" + roleName));
                    }
                }
            } catch (ParseException e) {
                log.error("Error parsing access token", e);
            }
            String usernameAttribute = userRequest.getClientRegistration().getProviderDetails()
                    .getUserInfoEndpoint().getUserNameAttributeName();
            return new DefaultOidcUser(mappedAuthorities, oidcUser.getIdToken(), oidcUser.getUserInfo(), usernameAttribute);
        };
    }
}
