package bamboo;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.OidcUserInfo;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;

import java.util.Set;

public class User extends DefaultOidcUser {
    private static final long serialVersionUID = 1L;

    public User(Set<GrantedAuthority> mappedAuthorities, OidcIdToken idToken, OidcUserInfo userInfo, String usernameAttribute) {
        super(mappedAuthorities, idToken, userInfo, usernameAttribute);
    }

    public Integer getAgencyId() {
        Long agencyId = getAttribute("agencyId");
        if (agencyId == null) return null;
        return (int)(long)agencyId;
    }

    public boolean hasAuthority(GrantedAuthority authority) {
        return this.getAuthorities().contains(authority);
    }
}
