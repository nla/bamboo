package bamboo.util;

public class Parsing {
    private Parsing() {}


    public static Long parseLongOrDefault(String s, long defaultValue) {
        return parseLongOrDefault(s, Long.valueOf(defaultValue));
    }
    public static Long parseLongOrDefault(String s, Long defaultValue) {
        if (s == null || s.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static String blankToNull(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }
        return s;
    }
}
