package bamboo.core;

import org.springframework.security.core.GrantedAuthority;

public enum Permission implements GrantedAuthority {
    /**
     * Permission to view crawl serieses in own agency.
     */
    SERIES_VIEW_AGENCY,
    /**
     * Permission to create or edit crawl serieses in own agency.
     */
    SERIES_EDIT_AGENCY,
    /**
     * Permission to view all crawl serieses regardless of agency.
     */
    SERIES_VIEW_ALL,
    /**
     * Permission to edit all crawl serieses regardless of agency.
     */
    SERIES_EDIT_ALL;

    @Override
    public String getAuthority() {
        return "PERM_" + this.name();
    }
}
