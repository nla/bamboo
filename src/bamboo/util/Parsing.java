package bamboo.util;

public class Parsing {
    private Parsing() {}

    public static Long parseLongOrDefault(String s, long defaultValue) {
        if (s == null) {
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
