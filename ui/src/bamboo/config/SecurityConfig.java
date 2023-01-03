package bamboo.config;

import bamboo.User;
import bamboo.core.Role;
import com.nimbusds.jwt.SignedJWT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserRequest;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserService;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.web.SecurityFilterChain;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.springframework.security.web.util.matcher.AntPathRequestMatcher.antMatcher;

@Configuration
public class SecurityConfig {
    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf().disable();
        String issuerUrl = System.getProperty("spring.security.oauth2.client.provider.oidc.issuer-uri");
        if (issuerUrl != null) {
            http.oauth2Login().userInfoEndpoint().oidcUserService(oidcUserService());
            http.logout().logoutSuccessUrl("/");
            http.oauth2ResourceServer().jwt()
                    .jwkSetUri(issuerUrl + "/protocol/openid-connect/certs")
                    .jwtAuthenticationConverter(jwt -> new JwtAuthenticationToken(jwt, mapClaimsToAuthorities(jwt.getClaims())));
            http.authorizeHttpRequests(authorize -> authorize
                    // static content
                    .requestMatchers(antMatcher("/webjars/**")).permitAll()
                    .requestMatchers(antMatcher("/assets/**")).permitAll()

                    // api: solr indexer
                    .requestMatchers(antMatcher("/collections/*/warcs/json")).permitAll()
                    .requestMatchers(antMatcher("/collections/*/warcs/sync")).permitAll()
                    .requestMatchers(antMatcher("/warcs/*/text")).permitAll()
                    // api: wayback
                    .requestMatchers(antMatcher("/warcs/*")).permitAll()

                    .requestMatchers(antMatcher("/")).hasRole(Role.STDUSER.name())
                    .requestMatchers(antMatcher("/series")).permitAll()
                    .requestMatchers(antMatcher("/series/**")).permitAll()
                    .requestMatchers(antMatcher("/crawls/**")).permitAll()

                    .requestMatchers(antMatcher("/api/**")).hasRole(Role.PANADMIN.name())

                    .anyRequest().hasRole(Role.PANADMIN.name()));
        }
        return http.build();
    }

    private Set<GrantedAuthority> mapClaimsToAuthorities(Map<String, Object> claims) {
        Set<GrantedAuthority> authorities = new HashSet<>();
        var claimedRoles = (List<String>) ((Map<String, Object>) claims.get("realm_access")).get("roles");
        for (Role role : Role.values()) {
            if (claimedRoles.contains(role.name().toLowerCase())) {
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
