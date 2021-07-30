package bamboo.util;

public class Units {
    public static String displaySize(long bytes) {
        long value = Math.abs(bytes);
        if (value < 1024) {
            return bytes + " bytes";
        }
        String units = "KMGTPE";
        int i;
        for (i = 0; i < units.length() - 1 && value > 1024 * 1024; i++) {
            value /= 1024;
        }
        return String.format("%.1f %ciB", value * Long.signum(bytes) / 1024.0, units.charAt(i));
    }
}
