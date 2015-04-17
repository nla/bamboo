package bamboo.util;

/**
 * User-defined functions for MySQL compatibility.
 */
public class H2Functions {
    public static String substringIndex(String s, String delim, int count) {
        if (s == null) {
            return null;
        }
        if (count > 0) {
            int pos = s.indexOf(delim);
            while (--count > 0 && pos != -1) {
                pos = s.indexOf(delim, pos + 1);
            }
            if (pos == -1) {
                return s;
            } else {
                return s.substring(0, pos);
            }
        } else if (count < 0) {
            int pos = s.lastIndexOf(delim);
            while (++count < 0 && pos != -1) {
                pos = s.lastIndexOf(delim, pos - 1);
            }
            if (pos == -1) {
                return s;
            } else {
                return s.substring(pos + 1);
            }
        } else {
            return "";
        }
    }
}
