package bamboo;

import bamboo.core.Permission;
import bamboo.core.Role;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Instances of this class are globally available as 'auth' in freemarker templates. See GlobalControllerAdvice.
 */
public class AuthHelper {
    private final PermissionEvaluator permissionEvaluator;

    public AuthHelper(PermissionEvaluator permissionEvaluator) {
        this.permissionEvaluator = permissionEvaluator;
    }

    public boolean hasRole(String role) {
        return hasAuthority(Role.valueOf(role.toUpperCase()));
    }

    public boolean isAuthenticated() {
        return currentUser() != null;
    }

    public boolean hasPermission(String permission) {
        return hasAuthority(Permission.valueOf(permission));
    }

    private boolean hasAuthority(GrantedAuthority authority) {
        return SecurityContextHolder.getContext().getAuthentication().getAuthorities().contains(authority);
    }

    public boolean hasPermission(Object object, String permission) {
        return permissionEvaluator.hasPermission(SecurityContextHolder.getContext().getAuthentication(), object, permission);
    }

    public String getUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) return "anonymous";
        return authentication.getName();
    }

    public static String currentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) return null;
        String authName = authentication.getName();
        if (authName == null || authName.equals("anonymousUser")) return null;
        return authName;
    }
}
