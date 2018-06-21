package bamboo.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Streams {

    private static final int BUF_SIZE = 16384;

    public static void copy(InputStream src, OutputStream dst) throws IOException {
        byte[] buf = new byte[BUF_SIZE];
        while (true) {
            int n = src.read(buf);
            if (n < 0) break;
            dst.write(buf, 0, n);
        }
    }

    public static void copy(InputStream src, OutputStream dst, long length) throws IOException {
        byte[] buf = new byte[BUF_SIZE];
        long remaining = length;
        while (remaining > 0) {
            int n = src.read(buf, 0, remaining < buf.length ? (int)remaining : buf.length);
            if (n < 0) break;
            dst.write(buf, 0, n);
            remaining -= n;
        }
    }
}
