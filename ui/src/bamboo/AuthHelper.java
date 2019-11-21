package bamboo;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.servlet.http.HttpServletRequest;

/**
 * Instances of this class are globally available as 'auth' in freemarker templates. See GlobalControllerAdvice.
 */
public class AuthHelper {
    private final HttpServletRequest request;

    public AuthHelper(HttpServletRequest request) {
        this.request = request;
    }

    public boolean hasRole(String role) {
        return request.isUserInRole(role);
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
