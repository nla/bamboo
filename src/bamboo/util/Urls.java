package bamboo.util;

import java.util.regex.Pattern;

public class Urls {
    private final static Pattern SCHEME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z+.-]*://?");

    public static String addImplicitScheme(String url) {
        return addImplicitScheme(url, "http");
    }

    public static String addImplicitScheme(String url, String scheme) {
        if (SCHEME_PATTERN.matcher(url).find()) {
            return url;
        } else {
            return scheme + "://" + url;
        }
    }
}
