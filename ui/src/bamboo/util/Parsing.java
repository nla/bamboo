package bamboo.util;

public class Parsing {
    private Parsing() {}


    public static int parseIntOrDefault(String s, int defaultValue) {
        if (s == null || s.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(s);
    }
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
