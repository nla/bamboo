package bamboo.task;

import org.archive.format.arc.ARCConstants;
import org.archive.util.LaxHttpParser;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpHeader {
    private static final Pattern HTTP_PREFIX = Pattern.compile("\\s*HTTP.*", Pattern.DOTALL | Pattern.MULTILINE);

    int status;
    String location;
    String rawLocation;
    String contentType;
    boolean brotli;

    public static HttpHeader parse(InputStream in, String targetUrl) throws IOException {
        String line = LaxHttpParser.readLine(in, "ISO-8859-1");
        if (line == null || !HTTP_PREFIX.matcher(line).matches()) {
            return null;
        }
        HttpHeader result = new HttpHeader();
        result.status = parseStatusLine(line);
        for (var header : LaxHttpParser.parseHeaders(in, ARCConstants.DEFAULT_ENCODING)) {
            switch (header.getName().toLowerCase()) {
                case "location":
                    try {
                        result.rawLocation = header.getValue();
                        URL url = new URL(targetUrl);
                        result.location = new URL(url, header.getValue()).toString().replace(" ", "%20");
                    } catch (MalformedURLException e) {
                        // skip it
                    }
                    break;
                case "content-type":
                    result.contentType = header.getValue();
                    break;
                case "content-encoding":
                    String[] encodings = header.getValue().split(",");
                    for (String encoding: encodings) {
                        if (encoding.trim().toLowerCase(Locale.ROOT).equals("br")) {
                            result.brotli = true;
                        }
                    }
            }
        }
        return result;
    }

    private static final Pattern STATUS_LINE = Pattern.compile("\\s*\\S+\\s+(\\d+)(?:\\s.*|$)", Pattern.DOTALL);

    static int parseStatusLine(String line) {
        Matcher m = STATUS_LINE.matcher(line);
        if (m.matches()) {
            return Integer.parseInt(m.group(1));
        } else {
            return -1;
        }
    }

    static String cleanContentType(String contentType) {
        if (contentType == null) return null;
        contentType = stripAfterChar(contentType, ';');
        contentType = stripAfterChar(contentType, ' ');
        switch (contentType) {
            case "application/xhtml+xml": return "text/html";
            case "application/x-pdf": return "application/pdf";
            default: return contentType;
        }
    }

    private static String stripAfterChar(String s, int c) {
        int i = s.indexOf(c);
        if (i > -1) {
            return s.substring(0, i);
        } else {
            return s;
        }
    }
}
