package bamboo;

import bamboo.config.BambooPermissionEvaluator;
import bamboo.core.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.ResponseStatus;

import jakarta.servlet.http.HttpServletRequest;

@ControllerAdvice
public class GlobalControllerAdvice {
    @Autowired
    private BambooPermissionEvaluator permissionEvaluator;

    @ModelAttribute("auth")
    public AuthHelper getAuth(@Autowired HttpServletRequest request) {
        return new AuthHelper(request, permissionEvaluator);
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handleNotFound(NotFoundException e, Model model, HttpServletRequest request) {
        model.addAttribute("status", 404);
        model.addAttribute("error", "Not Found");
        model.addAttribute("auth", getAuth(request));
        return "error";
    }
}
