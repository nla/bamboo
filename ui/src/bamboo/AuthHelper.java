package bamboo;

import bamboo.core.Permission;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Instances of this class are globally available as 'auth' in freemarker templates. See GlobalControllerAdvice.
 */
public class AuthHelper {
    private final HttpServletRequest request;
    private final PermissionEvaluator permissionEvaluator;

    public AuthHelper(HttpServletRequest request, PermissionEvaluator permissionEvaluator) {
        this.request = request;
        this.permissionEvaluator = permissionEvaluator;
    }

    public boolean hasRole(String role) {
        return request.isUserInRole(role.toUpperCase());
    }

    public boolean isAuthenticated() {
        return currentUser() != null;
    }

    public boolean hasPermission(String permission) {
        return request.isUserInRole(Permission.valueOf(permission).getAuthority());
    }

    public boolean hasPermission(Object object, String permission) {
        return permissionEvaluator.hasPermission(SecurityContextHolder.getContext().getAuthentication(), object, permission);
    }

    public User getUser() {
        return (User)request.getUserPrincipal();
    }

    public String getUsername() {
        return request.getUserPrincipal().getName();
    }

    public static String currentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) return null;
        String authName = authentication.getName();
        if (authName == null || authName.equals("anonymousUser")) return null;
        return authName;
    }
}
