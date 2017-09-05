package bamboo.util;

import spark.Request;
import spark.Response;
import spark.Spark;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.regex.Pattern;

public class Csrf {
    private static String HEADER = "X-Csrf-Token";
    private static String COOKIE = "bambooCsrfToken";
    private static String FORM_FIELD = "csrfToken";
    private static final Pattern RE_SANE_TOKEN = Pattern.compile("[a-zA-Z0-9_-]{20}");
    private static final SecureRandom random = new SecureRandom();

    public static Object token(Request request) {
        return request.attribute("csrfToken");
    }

    public static void protect(Request request, Response response) {
        String cookieToken = request.cookie(COOKIE);
        if (!isTokenSane(cookieToken)) {
            cookieToken = null;
        }
        if (!request.requestMethod().equals("GET") &&
                !request.requestMethod().equals("HEAD")) {
            if (cookieToken == null) {
                throw Spark.halt(400, "The cross-site request forgery protection cookie (" + COOKIE + ") is missing or invalid.  Ensure your browser has cookies enabled and refresh the form.");
            }
            String formToken = request.headers(HEADER);
            if (formToken == null) {
                formToken = request.queryParams(FORM_FIELD);
            }
            if (formToken == null || !formToken.equals(cookieToken)) {
                throw Spark.halt(400, "The cross-site request forgery protection form parameter (" + FORM_FIELD + ") is missing or invalid.");
            }
        }
        if (cookieToken == null) {
            cookieToken = generateToken();
            response.cookie(COOKIE, cookieToken, 60 * 60 * 24 * 7, request.protocol().equalsIgnoreCase("https"), false);
        }
        request.attribute("csrfToken", cookieToken);
    }

    private static String generateToken() {
        byte[] bytes = new byte[15];
        random.nextBytes(bytes);
        return Base64.getUrlEncoder().encodeToString(bytes);
    }

    private static boolean isTokenSane(String token) {
        return token != null && RE_SANE_TOKEN.matcher(token).matches();
    }
}
