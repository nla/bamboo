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
import java.util.List;

public class Scrub {

    enum ResultType {NEW, OK, MISMATCH, ERROR}

    ;

    public static class Result {
        public final ResultType type;
        public final long warcId;
        public final Path path;
        public final String storedDigest;
        public final String calculatedDigest;
        public final Exception error;

        Result(ResultType type, long warcId, Path path, String storedDigest, String calculatedDigest) {
            this.type = type;
            this.warcId = warcId;
            this.path = path;
            this.storedDigest = storedDigest;
            this.calculatedDigest = calculatedDigest;
            error = null;
        }

        Result(ResultType type, long warcId, Path path, String storedDigest, Exception error) {
            this.type = type;
            this.warcId = warcId;
            this.path = path;
            this.storedDigest = storedDigest;
            calculatedDigest = null;
            this.error = error;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "type=" + type +
                    ", id=" + warcId +
                    ", path=" + path +
                    ", storedDigest='" + storedDigest + '\'' +
                    ", calculatedDigest='" + calculatedDigest + '\'' +
                    ", error=" + error +
                    '}';
        }
    }

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

    static Result scrub(Db.Warc warc) {
        String digest;
        try {
            digest = calculateDigest("SHA-256", warc.path);
        } catch (IOException e) {
            return new Result(ResultType.ERROR, warc.id, warc.path, warc.sha256, e);
        }

        if (warc.sha256 == null) {
            return new Result(ResultType.NEW, warc.id, warc.path, digest, digest);
        } else if (warc.sha256.equals(digest)) {
            return new Result(ResultType.OK, warc.id, warc.path, digest, digest);
        } else {
            return new Result(ResultType.MISMATCH, warc.id, warc.path, warc.sha256, digest);
        }
    }

    public static void scrub(Bamboo bamboo) {
        List<Db.Warc> warcs;

        try (Db db = bamboo.dbPool.take()) {
            warcs = db.listWarcs();
        }

        warcs.parallelStream()
                .map(Scrub::scrub)
                .forEach(result -> {
                    System.out.println(result);
                    if (result.type == ResultType.NEW) {
                        try (Db db = bamboo.dbPool.take()) {
                            db.updateWarcSha256(result.warcId, result.calculatedDigest);
                        }
                    }
                });
    }
}
