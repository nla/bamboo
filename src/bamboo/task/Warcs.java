package bamboo.task;

import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.Base32;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Warcs {

    final static DateTimeFormatter warcDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    final static DateTimeFormatter arcDateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private Warcs() {
    }

    static boolean isResponseRecord(ArchiveRecordHeader h) {
        String warcType = (String)h.getHeaderValue("WARC-Type");
        if (warcType != null && !warcType.equals("response"))
            return false;
        if (h.getUrl().startsWith("dns:") || h.getUrl().startsWith("filedesc:"))
            return false;
        return true;
    }

    static String getCleanUrl(ArchiveRecordHeader h) {
        return h.getUrl().replace(" ", "%20");
    }

    static String getArcDate(ArchiveRecordHeader h) {
        return warcToArcDate(h.getDate());
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


    static String warcToArcDate(String warcDate) {
        if (warcDate.length() == 14) {
            return warcDate; // already an ARC date
        }
        return LocalDateTime.parse(warcDate, warcDateFormat).format(arcDateFormat);
    }
}
