package bamboo.core;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Scrub {
    static String calculateDigest(String algorithm, ReadableByteChannel channel) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance(algorithm);
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        while (channel.read(buffer) > 0) {
            buffer.flip();
            md.update(buffer);
            buffer.clear();
        }
        return DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
    }

    public static String calculateDigest(String algorithm, Path path) throws IOException {
        try (ReadableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
            return calculateDigest(algorithm, channel);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static void scrub(Db db) {
        for (Db.Warc warc : db.listWarcs()) {
            scrub(db, warc);
        }
    }

    private static void scrub(Db db, Db.Warc warc) {
        try {
            String digest = calculateDigest("SHA-256", warc.path);
            if (warc.sha256 == null) {
                System.out.println("NEW " + warc.id + " " + warc.path + " " + digest);
                db.updateWarcSha256(warc.id, digest);
            } else if (warc.sha256.equals(digest)) {
                System.out.println("OK " + warc.id + " " + warc.path + " " + digest);
            } else {
                System.out.println("MISMATCH " + warc.id + " " + warc.sha256 + " " + warc.path + " " + digest);
            }
        } catch (IOException e) {
            System.out.println("ERR " + warc.id + " " + warc.path + " " + e.toString());
        }
    }

    public static void scrub(Bamboo bamboo) {
        try (Db db = bamboo.dbPool.take()) {
            scrub(db);
        }
    }
}
