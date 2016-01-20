package bamboo.crawl;

import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.util.LaxHttpParser;
import org.archive.util.zip.GZIPMembersInputStream;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

public class RepairArc {

    static final byte[] DELETED_MARKER = "DELETED".getBytes(StandardCharsets.UTF_8);
    static final byte[] HTTP_MAGIC = "HTTP/1.".getBytes(StandardCharsets.UTF_8);
    static final byte[] FAKE_500_MAGIC = "500 ".getBytes(StandardCharsets.US_ASCII);

    enum FailureType {
        UNKNOWN, DELETED, COLON_IN_HTTP_STATUS, NO_HTTP_HEADER, JUNK_1KB_BEFORE_HEADER, JUNK_2KB_BEFORE_HEADER,
        JUNK_4KB_BEFORE_HEADER, JUNK_8KB_BEFORE_HEADER, FAKE_500
    };

    static int indexOf(byte[] a, byte b, int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] == b) {
                return i;
            }
        }
        return -1;
    }

    static FailureType determineFailureType(byte[] peek, int len) throws IOException {

        //
        // Record contents were replaced with something like:
        // DELETED_TIME=20000426101314_DELETER=Kurt_REASON=alexalist
        //
        if (Arrays.equals(DELETED_MARKER, Arrays.copyOfRange(peek, 0, DELETED_MARKER.length))) {
            return FailureType.DELETED;
        }

        //
        // 500 (Internal Server Error) Can't connect to www.hotmall.com.au:80 (Bad hostname 'www.hotmall.com.au')
        //
        if (len > FAKE_500_MAGIC.length && Arrays.equals(FAKE_500_MAGIC, Arrays.copyOfRange(peek, 0, FAKE_500_MAGIC.length))) {
            return FailureType.FAKE_500;
        }

        if (len > HTTP_MAGIC.length && Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 0, HTTP_MAGIC.length))) {
            //
            // HTTP status lines containing a colon like:
            // HTTP/1.0 404 Not Found: "/hbe/Yowies/spoon.jpg"
            //
            int firstColon = indexOf(peek, (byte) ':', len);
            int firstLinefeed = indexOf(peek, (byte) '\n', len);
            if (firstColon != -1 && (firstColon < firstLinefeed || firstLinefeed == -1)) {
                return FailureType.COLON_IN_HTTP_STATUS;
            }
        }

        if (len > HTTP_MAGIC.length && !Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 0, HTTP_MAGIC.length))) {
            //
            // Records with 1KB of junk before the HTTP headers
            //
            if (len > 1024 + HTTP_MAGIC.length && Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(peek, 1024, 1024 + HTTP_MAGIC.length))) {
                return FailureType.JUNK_1KB_BEFORE_HEADER;
            }

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

    static class Problem {
        public Problem(File file, FailureType type, long recordStart, long recordEnd, String arcHeader) {
            this.file = file;
            this.type = type;
            this.recordStart = recordStart;
            this.recordEnd = recordEnd;
            this.arcHeader = arcHeader;
            recordLength = Long.parseLong(arcHeader.split(" ")[4]);
        }

        @Override
        public String toString() {
            return "Problem{" +
                    "file=" + file +
                    ", type=" + type +
                    ", recordStart=" + recordStart +
                    ", recordEnd=" + recordEnd +
                    ", arcHeader='" + arcHeader + '\'' +
                    ", recordLength=" + recordLength +
                    '}';
        }

        final File file;
        final FailureType type;
        final long recordStart;
        final long recordEnd;
        final String arcHeader;
        final long recordLength;
    }

    static void transferCompletely(FileChannel in, long position, long count, FileChannel out) throws IOException {
        while (count > 0) {
            long written = in.transferTo(position, count, out);
            position += written;
            count -= written;
        }
    }

    static void removeBadRecord(Problem problem, Path dest) throws IOException {
        try (FileChannel in = FileChannel.open(problem.file.toPath(), StandardOpenOption.READ);
             FileChannel out = FileChannel.open(dest, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            transferCompletely(in, 0, problem.recordStart, out);
            transferCompletely(in, problem.recordEnd, in.size() - problem.recordEnd, out);
        }
    }

    static String readLine(InputStream in, int limit) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int i = 0; i < limit; i++) {
            int c = in.read();
            if (c == -1) break;
            baos.write(c);
            if (c == '\n') break;
        }
        return baos.toString("ISO-8859-1");
    }

    static void copy(InputStream in, OutputStream out, long count) throws IOException {
        byte[] buf = new byte[8192];
        while (count > 0) {
            int n = in.read(buf);
            if (n < 0) break;
            out.write(buf, 0, n);
            count -= n;
        }
    }

    static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        for (;;) {
            int n = in.read(buf);
            if (n < 0) break;
            out.write(buf, 0, n);
        }
    }

    static int detectKbAlignedHeader(byte[] buffer, int buflen, String arcHeader, String statusLine) {
        //System.out.println("bound check " + statusLine);
        for (int boundary = 1024; boundary < 8192; boundary += 1024) {
            if (buflen > boundary + HTTP_MAGIC.length && Arrays.equals(HTTP_MAGIC, Arrays.copyOfRange(buffer, boundary, boundary + HTTP_MAGIC.length))) {
                return boundary;
            }
        }
        return 0;
    }

    static String subtractLengthFromHeader(String arcHeader, long delta) {
        int sp = arcHeader.lastIndexOf(' ');
        long length = Long.parseLong(arcHeader.substring(sp + 1).trim());
        length -= delta;
        return arcHeader.substring(0, sp) + " " + length + "\n";
    }

    static void fixStatusColon(Problem problem, Path src, Path dest) throws IOException {
        int httpStart;
        long recordsModified = 0, recordsCopied = 0, recordsDiscarded = 0;
        byte[] buffer = new byte[16384];
        try (FileChannel in = FileChannel.open(src, StandardOpenOption.READ);
             FileChannel in2 = FileChannel.open(src, StandardOpenOption.READ);
             GZIPMembersInputStream gzip = new GZIPMembersInputStream(Channels.newInputStream(in));
             FileChannel out = FileChannel.open(dest, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            gzip.setEofEachMember(true);
            for (;;) {
                String arcHeader = readLine(gzip, 65536);
                if (arcHeader.isEmpty()) {
                    assert(gzip.getCurrentMemberStart() == in2.size());
                    break;
                }
                int buflen = 0;
                while (buflen < buffer.length) {
                    int n = gzip.read(buffer, buflen, buffer.length - buflen);
                    if (n < 0) break;
                    buflen += n;
                }
                int linebreak = indexOf(buffer, (byte) '\n', buflen);
                String statusLine = new String(buffer, 0, linebreak, StandardCharsets.ISO_8859_1);
                String trimmed = statusLine.trim();

                if (statusLine.startsWith("HTTP/1.") && statusLine.contains(":")) {
                    System.out.println("Replace colon from " + gzip.getCurrentMemberStart());
                    statusLine = statusLine.replace(':', ' ');
                    byte[] newStatus = statusLine.getBytes(StandardCharsets.ISO_8859_1);
                    GZIPOutputStream gzipOut = new GZIPOutputStream(Channels.newOutputStream(out));
                    gzipOut.write(arcHeader.getBytes(StandardCharsets.ISO_8859_1));
                    gzipOut.write(newStatus);
                    gzipOut.write(buffer, newStatus.length, buflen - newStatus.length);
                    copy(gzip, gzipOut);
                    gzipOut.finish();
                    gzip.nextMember();
                    recordsModified++;

                } else if (!statusLine.startsWith("HTTP/") && (httpStart = detectKbAlignedHeader(buffer, buflen, arcHeader, statusLine)) > 0) {
                    System.out.println(gzip.getCurrentMemberStart() + " " + arcHeader);
                    System.out.println("HTTP response prefixed with junk detected at " + httpStart + ". Trimming junk from record.");

                    arcHeader = subtractLengthFromHeader(arcHeader, httpStart);

                    GZIPOutputStream gzipOut = new GZIPOutputStream(Channels.newOutputStream(out));
                    gzipOut.write(arcHeader.getBytes(StandardCharsets.ISO_8859_1));
                    gzipOut.write(buffer, httpStart, buflen - httpStart);
                    copy(gzip, gzipOut);
                    gzipOut.finish();
                    gzip.nextMember();
                    recordsModified++;

                } else if (statusLine.startsWith("500 (Internal Server Error) Can't connect to ") || statusLine.startsWith("DELETED_TIME=")) {
                    if (statusLine.startsWith("500")) {
                        System.out.println("Discard fake 500 error");
                    } else {
                        System.out.println("Discard deleted record");
                    }
                    // skip to end of record, discarding the data
                    while (!gzip.getAtMemberEnd()) {
                        int n = gzip.read(buffer);
                    }
                    gzip.nextMember();
                    recordsDiscarded++;

                } else if (trimmed.equals("") || trimmed.equals("1") || !trimmed.startsWith("HTTP")) {
                    System.out.println("XXX '" + statusLine + "' " + gzip.getCurrentMemberStart());
                    // skip to end of record, discarding the data as we'll copy the compressed stream
                    while (!gzip.getAtMemberEnd()) {
                        int n = gzip.read(buffer);
                    }

                    if (gzip.getCurrentMemberEnd() == in2.size()) {
                        System.out.println("Discarding record with bogus status line at end of ARC");
                        recordsDiscarded++;
                    } else {
                        long start = gzip.getCurrentMemberStart();
                        long lengthToCopy = gzip.getCurrentMemberEnd() - gzip.getCurrentMemberStart();
                        transferCompletely(in2, start, lengthToCopy, out);
                        recordsCopied++;
                    }
                    gzip.nextMember();
                } else {
                    // skip to end of record, discarding the data as we'll copy the compressed stream
                    while (!gzip.getAtMemberEnd()) {
                        int n = gzip.read(buffer);
                    }

                    long start = gzip.getCurrentMemberStart();
                    long lengthToCopy = gzip.getCurrentMemberEnd() - gzip.getCurrentMemberStart();
                    transferCompletely(in2, start, lengthToCopy, out);
                    gzip.nextMember();
                    recordsCopied++;
                }
            }
        }
        System.out.println("Modified " + recordsModified + " records. Copied " + recordsCopied + " as is. Discarded " + recordsDiscarded);
    }

    public static void main(String args[]) throws IOException {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }

        if (args[0].equals("-f")) {
            fixStatusColon(null, Paths.get(args[1]), Paths.get(args[2]));
            return;
        }



        File file = new File(args[0]);
        long lastOkRecord = 0;
        try (ARCReader reader = ARCReaderFactory.get(file)) {
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

        Problem problem;

        try (FileInputStream fis = new FileInputStream(file)) {
            fis.skip(lastOkRecord);
            GZIPMembersInputStream in = new GZIPMembersInputStream(fis);
            skipToEndOfRecord(in);

            String arcHeader = LaxHttpParser.readLine(in, "UTF-8");
            long recordStart = lastOkRecord + in.getCurrentMemberStart();
            long recordLength = Long.parseLong(arcHeader.split(" ")[4]);

            // grab the first few KB of the record for analysis
            byte peek[] = new byte[16384];
            int peekLen = 0;
            while (peekLen < peek.length && !in.getAtMemberEnd() && peekLen < recordLength) {
                peekLen += in.read(peek, peekLen, (int)Math.min(peek.length - peekLen, recordLength - peekLen));
            }

            // skip the remaining part of the record
            long pos = peekLen;
            while (pos < recordLength) {
                long skipped = in.skip(recordLength - pos);
                if (skipped < 0) break;
                pos += skipped;
            }

            long recordEnd;
            if (in.getAtMemberEnd()) {
                recordEnd = in.getCurrentMemberEnd();
            } else {
                int b1 = in.read();
                int b2 = in.read();
                if (b1 < 0 || b2 < 0) {
                    // end of file
                    recordEnd = file.length();
                } else if (b1 == '\n') {
                    recordEnd = lastOkRecord + in.getCurrentMemberStart();
                } else {
                    // recover by reading until end of gzip member?
                    throw new IllegalStateException("ARC record length appears to be wrong [" + b2 + "]");
                }
            }

            FailureType type = determineFailureType(peek, peekLen);
            problem = new Problem(file, type, recordStart, recordEnd, arcHeader);

            System.out.println(problem);
            System.out.println("END " + recordEnd);

            if (args.length >= 2) {
                Path dest = Paths.get(args[1]);
                System.out.println("Writing repaired ARC to " + dest);
                removeBadRecord(problem, dest);
            }

        }

    }

}
