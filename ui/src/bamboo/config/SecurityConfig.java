package bamboo.config;

import bamboo.User;
import bamboo.core.Role;
import com.nimbusds.jwt.SignedJWT;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserRequest;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserService;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.text.ParseException;
import java.util.*;

@Configuration
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        String issuerUrl = System.getProperty("spring.security.oauth2.client.provider.oidc.issuer-uri");
        if (issuerUrl != null) {
            http.oauth2Login().userInfoEndpoint().oidcUserService(oidcUserService());
            http.logout().logoutSuccessUrl("/");
            http.oauth2ResourceServer().jwt()
                    .jwkSetUri(issuerUrl + "/protocol/openid-connect/certs")
                    .jwtAuthenticationConverter(jwt -> new JwtAuthenticationToken(jwt, mapClaimsToAuthorities(jwt.getClaims())));
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

                    .antMatchers("/").hasRole(Role.STDUSER.name())
                    .antMatchers("/series").permitAll()
                    .antMatchers("/series/**").permitAll()
                    .antMatchers("/crawls/**").permitAll()

                    .antMatchers("/api/**").hasRole(Role.PANADMIN.name())


                    .anyRequest().hasRole(Role.PANADMIN.name());
        }
    }

    private Set<GrantedAuthority> mapClaimsToAuthorities(Map<String, Object> claims) {
        Set<GrantedAuthority> authorities = new HashSet<>();
        JSONArray array = (JSONArray) (((JSONObject) claims.get("realm_access")).get("roles"));
        HashSet<Object> clamedRoles = new HashSet<>(array);
        for (Role role : Role.values()) {
            if (clamedRoles.contains(role.name().toLowerCase())) {
                authorities.add(role);
                authorities.addAll(role.getPermissions());
            }
        }
        return authorities;
    }

    private OAuth2UserService<OidcUserRequest, OidcUser> oidcUserService() {
        OidcUserService delegate = new OidcUserService();
        return (userRequest) -> {
            OidcUser oidcUser = delegate.loadUser(userRequest);
            String accessTokenValue = userRequest.getAccessToken().getTokenValue();
            Set<GrantedAuthority> mappedAuthorities = new HashSet<>(oidcUser.getAuthorities());
            try {
                Map<String, Object> decoded = SignedJWT.parse(accessTokenValue).getPayload().toJSONObject();
                mappedAuthorities.addAll(mapClaimsToAuthorities(decoded));
            } catch (ParseException e) {
                log.error("Error parsing access token", e);
            }
            String usernameAttribute = userRequest.getClientRegistration().getProviderDetails()
                    .getUserInfoEndpoint().getUserNameAttributeName();
            return new User(mappedAuthorities, oidcUser.getIdToken(), oidcUser.getUserInfo(), usernameAttribute);
        };
    }
}
