package bamboo.crawl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.xml.bind.DatatypeConverter;

import bamboo.app.Bamboo;

public class Scrub {

    private final Bamboo bamboo;

    public Scrub(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

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

    Result scrub(Warc warc) {
        String digest;
        try {
            digest = calculateDigest("SHA-256", bamboo.warcs.openChannel(warc));
        } catch (IOException | NoSuchAlgorithmException e) {
            return new Result(ResultType.ERROR, warc.getId(), warc.getPath(), warc.getSha256(), e);
        }

        if (warc.getSha256() == null) {
            return new Result(ResultType.NEW, warc.getId(), warc.getPath(), digest, digest);
        } else if (warc.getSha256().equals(digest)) {
            return new Result(ResultType.OK, warc.getId(), warc.getPath(), digest, digest);
        } else {
            return new Result(ResultType.MISMATCH, warc.getId(), warc.getPath(), warc.getSha256(), digest);
        }
    }

    void scrub() {
        List<Warc> warcs = bamboo.warcs.listAll();

        warcs.parallelStream()
                .map(this::scrub)
                .forEach(result -> {
                    System.out.println(result);
                    if (result.type == ResultType.NEW) {
                        bamboo.warcs.updateSha256(result.warcId, result.calculatedDigest);
                    }
                });
    }

    public static void scrub(Bamboo bamboo) {
        new Scrub(bamboo).scrub();
    }
}
