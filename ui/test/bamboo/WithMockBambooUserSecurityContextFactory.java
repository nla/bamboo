package bamboo;

import bamboo.core.Role;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.OidcUserInfo;
import org.springframework.security.test.context.support.WithSecurityContextFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WithMockBambooUserSecurityContextFactory implements WithSecurityContextFactory<WithMockBambooUser> {
    @Override
    public SecurityContext createSecurityContext(WithMockBambooUser annotation) {
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        Set<GrantedAuthority> authoritySet = new HashSet<>();
        authoritySet.add(Role.PANADMIN);
        authoritySet.addAll(Role.PANADMIN.getPermissions());
        User principal = new User(authoritySet, new OidcIdToken("A", Instant.now(), Instant.now(), Map.of("username", "mockuser")),
                new OidcUserInfo(Map.of("username", "mockuser")), "username");
        Authentication auth = new UsernamePasswordAuthenticationToken(principal, "password", principal.getAuthorities());
        context.setAuthentication(auth);
        return context;
    }
}
