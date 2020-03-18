package bamboo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
public class GlobalControllerAdvice {
    @ModelAttribute("auth")
    public AuthHelper getAuth(@Autowired HttpServletRequest request, @Autowired BambooPermissionEvaluator permissionEvaluator) {
        return new AuthHelper(request, permissionEvaluator);
    }
}
