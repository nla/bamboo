package bamboo.config;

import bamboo.User;
import bamboo.app.Bamboo;
import bamboo.core.Role;
import bamboo.crawl.Crawl;
import bamboo.crawl.Series;
import bamboo.crawl.Warc;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Collection;

import static bamboo.core.Permission.*;

/**
 * Implements hasPermission() in @PreAuthorize and @PostAuthorize annotations.
 */
@Component
public class BambooPermissionEvaluator implements PermissionEvaluator {
    final Bamboo bamboo;

    public BambooPermissionEvaluator(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    @Override
    public boolean hasPermission(Authentication authentication, Object target, Object permission) {
        Collection<? extends GrantedAuthority> authorities = authentication.getAuthorities();
        if (authorities.contains(new SimpleGrantedAuthority("ROLE_MOCKADMIN"))) return true;
        Object principal = authentication.getPrincipal();
        User user = principal instanceof User ? (User) principal : null;
        String qualified = target.getClass().getSimpleName() + ":" + permission;
        switch (qualified) {
            case "Series:edit":
                if (authorities.contains(SERIES_EDIT_ALL)) {
                    return true;
                } else if (authorities.contains(SERIES_EDIT_AGENCY)) {
                    Integer seriesAgencyId = ((Series)target).getAgencyId();
                    return seriesAgencyId != null && seriesAgencyId.equals(user.getAgencyId());
                }
                break;
            case "Series:view":
                if (authorities.contains(SERIES_VIEW_ALL)) {
                    return true;
                } else if (authorities.contains(SERIES_VIEW_AGENCY)) {
                    Integer seriesAgencyId = ((Series)target).getAgencyId();
                    return seriesAgencyId != null && seriesAgencyId.equals(user.getAgencyId());
                }
                break;
            case "Crawl:edit":
            case "Crawl:view":
                if (authorities.contains(SERIES_VIEW_ALL)) return true;
                return hasPermission(authentication, ((Crawl)target).getCrawlSeriesId(), "Series", "view");
            case "Warc:edit":
                return hasPermission(authentication, ((Warc)target).getCrawlId(), "Crawl", "edit");
            default:
                throw new IllegalArgumentException("Unknown permission: " + qualified);
        }
        return false;
    }

    @Override
    public boolean hasPermission(Authentication authentication, Serializable targetId, String targetType, Object permission) {
        if (authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_MOCKADMIN"))) return true;
        switch (targetType) {
            case "Crawl":
                return hasPermission(authentication, bamboo.crawls.get((Long) targetId), permission);
            case "Series":
                return hasPermission(authentication, bamboo.serieses.get((Long) targetId), permission);
            case "Warc":
                return hasPermission(authentication, bamboo.warcs.get((Long) targetId), permission);
            default:
                throw new IllegalArgumentException("Permissions for " + targetType + " not implemented");
        }
    }
}
