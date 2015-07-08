package bamboo.core;

import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.util.LaxHttpParser;
import org.archive.util.zip.GZIPMembersInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RepairArc {

    static final byte[] DELETED_MARKER = "DELETED".getBytes(StandardCharsets.UTF_8);
    static final byte[] HTTP_MAGIC = "HTTP/1.".getBytes(StandardCharsets.UTF_8);

    enum FailureType {
        UNKNOWN, DELETED, COLON_IN_HTTP_STATUS, NO_HTTP_HEADER, JUNK_2KB_BEFORE_HEADER, JUNK_4KB_BEFORE_HEADER, JUNK_8KB_BEFORE_HEADER,
    };

    static int indexOf(byte[] a, byte b, int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] == b) {
                return i;
            }
        }
        return -1;
    }

    static FailureType determineFailureType(GZIPMembersInputStream in) throws IOException {
        byte peek[] = new byte[16384];
        int len = 0;
        while (len < peek.length && !in.getAtMemberEnd()) {
            len += in.read(peek, len, peek.length - len);
        }

        //
        // Record contents were replaced with something like:
        // DELETED_TIME=20000426101314_DELETER=Kurt_REASON=alexalist
        //
        if (Arrays.equals(DELETED_MARKER, Arrays.copyOfRange(peek, 0, DELETED_MARKER.length))) {
            return FailureType.DELETED;
        }

        //
        // HTTP status lines containing a colon like:
        // HTTP/1.0 404 Not Found: "/hbe/Yowies/spoon.jpg"
        //
        int firstColon = indexOf(peek, (byte)':', len);
        int firstLinefeed = indexOf(peek, (byte)'\n', len);
        if (firstColon != -1 && (firstColon < firstLinefeed || firstLinefeed == -1)) {
            return FailureType.COLON_IN_HTTP_STATUS;
        }

        if (len > HTTP_MAGIC.length && !Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 0, HTTP_MAGIC.length))) {

            //
            // Records with 2KB of junk before the HTTP headers
            //
            if (len > 2048 + HTTP_MAGIC.length && Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 2048, 2048 + HTTP_MAGIC.length))) {
                return FailureType.JUNK_2KB_BEFORE_HEADER;
            }

            //
            // Records with 4KB of junk before the HTTP headers
            //
            if (len > 4096 + HTTP_MAGIC.length && Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 4096, 4096 + HTTP_MAGIC.length))) {
                return FailureType.JUNK_4KB_BEFORE_HEADER;
            }

            //
            // Records with 8KB of junk before the HTTP headers
            //
            if (len > 8096 + HTTP_MAGIC.length && Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 8096, 8096 + HTTP_MAGIC.length))) {
                return FailureType.JUNK_8KB_BEFORE_HEADER;
            }

            //
            // No HTTP headers, just HTML
            //
            for (int i = 0; i < len; i++) {
                if (peek[i] == '\r' || peek[i] == '\n') {
                    continue;
                } else if (peek[i] == '<') {
                    return FailureType.NO_HTTP_HEADER;
                } else {
                    break;
                }
            }

        }


        return FailureType.UNKNOWN;

    }

    static void skipToEndOfRecord(InputStream in) throws IOException {
        String arcHeader = LaxHttpParser.readLine(in, "UTF-8");
        long toSkip = Long.parseLong(arcHeader.split(" ")[4]);
        while (toSkip > 0) {
            toSkip -= in.skip(toSkip);
        }
        String trailingLinefeed = LaxHttpParser.readLine(in, "UTF-8");
        assert(trailingLinefeed.equals(""));
    }

    static void usage() {
        System.out.println("Usage: RepairArc file.arc.gz");
        System.out.println("Classifies problems preventing an ARC from being read");
    }

    public static void main(String args[]) throws IOException {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }

        File path = new File(args[0]);
        long lastOkRecord = 0;
        try (ARCReader reader = ARCReaderFactory.get(path)) {
            for (ArchiveRecord record : reader) {
                // read all records until we encounter an error
                ArchiveRecordHeader header = record.getHeader();
                lastOkRecord = header.getOffset();
            }
            System.out.println("No problem");
            return;
        } catch (RuntimeException e) {
            System.err.println("Encountered ARCReader error after " + lastOkRecord);
            e.printStackTrace();
        }

        try (FileInputStream fis = new FileInputStream(path)) {
            fis.skip(lastOkRecord);
            GZIPMembersInputStream in = new GZIPMembersInputStream(fis);
            skipToEndOfRecord(in);

            String arcHeader = LaxHttpParser.readLine(in, "UTF-8");
            System.out.println(arcHeader);

            long startOfBadRecord = lastOkRecord + in.getCurrentMemberStart();

            FailureType type = determineFailureType(in);
            System.out.println(type + " record starts at " + startOfBadRecord);
        }
    }

}
