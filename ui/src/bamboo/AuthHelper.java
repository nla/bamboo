package bamboo;

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
}
