package bamboo.virus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;

/** Minimal client for clamd's NUL-framed Unix socket protocol. */
public class ClamdClient {
    private static final int CHUNK_SIZE = 64 * 1024;
    public static final long DEFAULT_MAX_STREAM_LENGTH = 100L * 1024 * 1024;
    private final Path socketPath;
    private final long maxStreamLength;

    public ClamdClient(Path socketPath) {
        this(socketPath, DEFAULT_MAX_STREAM_LENGTH);
    }

    ClamdClient(Path socketPath, long maxStreamLength) {
        this.socketPath = socketPath;
        if (maxStreamLength < 1) throw new IllegalArgumentException("maxStreamLength must be positive");
        this.maxStreamLength = maxStreamLength;
    }

    public String version() throws IOException {
        return command("VERSION");
    }

    public ScanSession openSession() throws IOException {
        SocketChannel channel = connect();
        try {
            Session session = new Session(channel);
            session.writeCommand("IDSESSION");
            return session;
        } catch (Throwable t) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            throw t;
        }
    }

    private class Session implements ScanSession {
        private final SocketChannel channel;
        private final InputStream response;
        private final OutputStream request;
        private int requestId;

        Session(SocketChannel channel) {
            this.channel = channel;
            response = Channels.newInputStream(channel);
            request = Channels.newOutputStream(channel);
        }

        @Override
        public ScanResult scan(InputStream input) throws IOException {
            writeCommand("INSTREAM");

            byte[] chunk = new byte[CHUNK_SIZE];
            long bytes = 0;
            int length;
            while (bytes < maxStreamLength &&
                    (length = input.read(chunk, 0, (int) Math.min(chunk.length, maxStreamLength - bytes))) != -1) {
                if (length == 0) continue;
                request.write(ByteBuffer.allocate(4).putInt(length).array());
                request.write(chunk, 0, length);
                bytes += length;
            }
            request.write(new byte[4]);
            request.flush();

            String reply = sessionReply(readRecord(response), ++requestId);
            if (reply.endsWith(" OK")) {
                return new ScanResult(Status.CLEAN, null, reply, bytes);
            }
            if (reply.endsWith(" FOUND")) {
                int separator = reply.indexOf(": ");
                String signature = separator < 0
                        ? reply.substring(0, reply.length() - " FOUND".length())
                        : reply.substring(separator + 2, reply.length() - " FOUND".length());
                return new ScanResult(Status.FOUND, signature, reply, bytes);
            }
            return new ScanResult(Status.ERROR, null, reply, bytes);
        }

        private void writeCommand(String command) throws IOException {
            request.write(('z' + command + "\0").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            request.flush();
        }

        @Override
        public void close() {
            try {
                writeCommand("END");
            } catch (IOException ignored) {
            }
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }

    private String command(String command) throws IOException {
        try (SocketChannel channel = connect();
             InputStream response = Channels.newInputStream(channel);
             OutputStream request = Channels.newOutputStream(channel)) {
            request.write(('z' + command + "\0").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            request.flush();
            return readRecord(response);
        }
    }

    private SocketChannel connect() throws IOException {
        SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
        try {
            channel.connect(UnixDomainSocketAddress.of(socketPath));
            return channel;
        } catch (Throwable t) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            throw t;
        }
    }

    private static String readRecord(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = input.read(buffer)) != -1) {
            int nul = -1;
            for (int i = 0; i < length; i++) {
                if (buffer[i] == 0) {
                    nul = i;
                    break;
                }
            }
            output.write(buffer, 0, nul < 0 ? length : nul);
            if (nul >= 0) break;
        }
        if (output.size() == 0) throw new IOException("clamd closed the connection without a reply");
        return output.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String sessionReply(String reply, int expectedId) throws IOException {
        String prefix = expectedId + ": ";
        if (!reply.startsWith(prefix)) {
            throw new IOException("Unexpected clamd session reply: " + reply);
        }
        return reply.substring(prefix.length());
    }

    public enum Status { CLEAN, FOUND, ERROR }

    public record ScanResult(Status status, String signature, String reply, long bytes) {
    }

    public interface ScanSession extends AutoCloseable {
        ScanResult scan(InputStream input) throws IOException;

        @Override
        void close();
    }
}
