package bamboo.util;

public class Parsing {
    private Parsing() {}

    public static Long parseLongOrNull(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
