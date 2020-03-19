package bamboo.core;

import org.springframework.security.core.GrantedAuthority;

import java.util.Set;

import static bamboo.core.Permission.*;

public enum Role implements GrantedAuthority {
    INFOUSER(),
    STDUSER(SERIES_VIEW_AGENCY),
    AGADMIN(SERIES_EDIT_AGENCY),
    PANADMIN(SERIES_EDIT_ALL, SERIES_VIEW_ALL),
    SYSADMIN(SERIES_EDIT_ALL, SERIES_VIEW_ALL);

    private final Set<Permission> permissions;

    Role(Permission... permissions) {
        this.permissions = Set.of(permissions);
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }

    @Override
    public String getAuthority() {
        return "ROLE_" + name();
    }
}
