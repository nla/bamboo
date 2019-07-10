package bamboo.task;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.util.Base32;
import org.netpreserve.urlcanon.Canonicalizer;
import org.netpreserve.urlcanon.ParsedUrl;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

public class WarcUtils {

    public final static DateTimeFormatter warcDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    public final static DateTimeFormatter arcDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);

    private WarcUtils() {
    }

    static boolean isResponseRecord(ArchiveRecordHeader h) {
        String warcType = (String)h.getHeaderValue("WARC-Type");
        if (warcType != null && !warcType.equals("response"))
            return false;
        if (h.getUrl().startsWith("dns:") || h.getUrl().startsWith("filedesc:") || h.getUrl().startsWith("whois:"))
            return false;
        return true;
    }

    static boolean isResourceRecord(ArchiveRecordHeader h) {
        String warcType = (String)h.getHeaderValue("WARC-Type");
        if (warcType != null && warcType.equals("resource")) {
            return true;
        }
        return false;
    }

    static boolean isMetadataRecord(ArchiveRecordHeader h) {
        String warcType = (String)h.getHeaderValue("WARC-Type");
        if (warcType != null && warcType.equals("metadata")) {
            return true;
        }
        return false;
    }

    static String cleanUrl(String url) {
        String canon;
        try {
            ParsedUrl parsedUrl = ParsedUrl.parseUrl(url);
            Canonicalizer.WHATWG.canonicalize(parsedUrl);
            canon = parsedUrl.toString();
        } catch (Exception e) {
            canon = url.replaceFirst("#.*", "");
        }
        return canon.replace(" ", "%20").replace("\r", "%0a").replace("\n", "%0d");
    }

    static String getCleanUrl(ArchiveRecordHeader h) {
        return cleanUrl(h.getUrl());
    }

    static String getArcDate(ArchiveRecordHeader h) {
        return warcToArcDate(repairCorruptArcDate(h.getDate()));
    }

    static String getOrCalcDigest(ArchiveRecord record) throws IOException {
        String digest = (String) record.getHeader().getHeaderValue("WARC-Payload-Digest");
        if (digest == null) {
            return calcDigest(record);
        } else if (digest.startsWith("sha1:")) {
            return digest.substring(5);
        } else {
            return digest;
        }
    }

    static String calcDigest(ArchiveRecord record) throws IOException {
        String digest;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            byte[] buf = new byte[8192];
            for (; ; ) {
                int len = record.read(buf);
                if (len < 0) break;
                md.update(buf, 0, len);
            }
            digest = Base32.encode(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return digest;
    }

    /**
     * We've encountered some ARC files collected circa 2000 which have corrupt dates. It appears there was a data
     * race in the software that originally wrote them. This attempts to repair them.
     */
    static String repairCorruptArcDate(String arcDate) {
        if (arcDate.length() == 16) {
            // Dates with an extra 00 inserted somewhere after the year.
            // 200010[00]01063618
            // 200010010830[00]07
            // 200010[00]01063618
            int i = 4; // always seems to be after the year
            do {
                i = arcDate.indexOf("00", i);
                if (i % 2 == 0) {
                    String candidate = arcDate.substring(0, i) + arcDate.substring(i + 2);
                    try {
                        parseArcDate(candidate);
                        return candidate;
                    } catch (DateTimeParseException e) {
                        // try removing a later occurance
                    }
                }
                i++;
            } while (i >= 0 && i < arcDate.length());
            // give up, chop the end off
            return arcDate.substring(0, 14);
        } else if (arcDate.length() == 18) {
            // Dates with two extra 00 inserted somewhere after the year
            // 2000083115[00]55[00]15
            int i = 4; // always seems to be after the year
            do {
                i = arcDate.indexOf("00", i);
                if (i % 2 == 0) {
                    int j = i + 2;
                    do {
                        j = arcDate.indexOf("00", j);
                        if (j % 2 == 0) {
                            String candidate = arcDate.substring(0, i) + arcDate.substring(i + 2, j) + arcDate.substring(j + 2);

                            try {
                                parseArcDate(candidate);
                                return candidate;
                            } catch (DateTimeParseException e) {
                                // try removing a later occurance
                            }
                        }
                        j++;
                    } while (j >= 0 && j < arcDate.length());
                }
                i++;
            } while (i >= 0 && i < arcDate.length());
            // give up, chop the end off
            return arcDate.substring(0, 14);
        } else if (arcDate.length() == 12) {
            // pad out truncated dates
            return arcDate + "00";
        }
        return arcDate;
    }


    static String warcToArcDate(String warcDate) {
        if (warcDate.length() == 14) {
            return warcDate; // already an ARC date
        }
        return LocalDateTime.parse(warcDate, warcDateFormat).format(arcDateFormat);
    }

    public static Date parseArcDate(String arcDate) {
        LocalDateTime parsed = LocalDateTime.parse(arcDate, arcDateFormat);
        return Date.from(parsed.toInstant(ZoneOffset.UTC));
    }

    public static ArchiveReader open(Path path) throws IOException {
        /*
         * ArchiveReaderFactor.get doesn't understand the .open extension.
         */
        if (path.toString().endsWith(".warc.gz.open")) {
            return WARCReaderFactory.get(path.toFile());
        } else {
            return ArchiveReaderFactory.get(path.toFile());
        }
    }
}
