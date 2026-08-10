package bamboo.virus;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ClamdClientTest {
    @Test
    public void scansViaUnixSocket() throws Exception {
        Path directory = Files.createTempDirectory(Path.of("/tmp"), "clamd-test-");
        Path socket = directory.resolve("clamd.sock");
        try {
            try (FakeClamd server = new FakeClamd(socket)) {
                ClamdClient client = new ClamdClient(socket, 5);
                assertEquals("ClamAV test/123/Mon Aug 10", client.version());

                try (ClamdClient.ScanSession session = client.openSession()) {
                    ClamdClient.ScanResult clean = session.scan(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
                    assertEquals(ClamdClient.Status.CLEAN, clean.status());
                    assertEquals(5, clean.bytes());

                    ClamdClient.ScanResult found = session.scan(new ByteArrayInputStream("virus-and-more".getBytes(StandardCharsets.UTF_8)));
                    assertEquals(ClamdClient.Status.FOUND, found.status());
                    assertEquals("Test.Signature", found.signature());
                    assertEquals(5, found.bytes());
                }

                assertEquals(2, server.connections.get()); // VERSION plus one IDSESSION
            }
        } finally {
            Files.deleteIfExists(socket);
            Files.deleteIfExists(directory);
        }
    }

    private static class FakeClamd implements AutoCloseable {
        private final ServerSocketChannel server;
        private final Thread thread;
        private final AtomicInteger connections = new AtomicInteger();
        private volatile IOException failure;

        FakeClamd(Path socket) throws IOException {
            Files.deleteIfExists(socket);
            server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            server.bind(UnixDomainSocketAddress.of(socket));
            thread = new Thread(this::serve, "fake-clamd");
            thread.setDaemon(true);
            thread.start();
        }

        private void serve() {
            try {
                while (server.isOpen()) {
                    try (SocketChannel client = server.accept()) {
                        connections.incrementAndGet();
                        String command = readCommand(client);
                        if (command.equals("zVERSION")) {
                            reply(client, "ClamAV test/123/Mon Aug 10");
                        } else if (command.equals("zIDSESSION")) {
                            serveSession(client);
                        } else {
                            reply(client, "UNKNOWN COMMAND ERROR");
                        }
                    }
                }
            } catch (IOException e) {
                if (server.isOpen()) failure = e;
            }
        }

        private static void serveSession(SocketChannel client) throws IOException {
            int requestId = 0;
            while (true) {
                String command = readCommand(client);
                if (command.equals("zEND")) return;
                requestId++;
                if (command.equals("zINSTREAM")) {
                    String content = new String(readChunks(client), StandardCharsets.UTF_8);
                    reply(client, requestId + ": " + (content.contains("virus")
                            ? "stream: Test.Signature FOUND"
                            : "stream: OK"));
                } else {
                    reply(client, requestId + ": UNKNOWN COMMAND ERROR");
                }
            }
        }

        private static String readCommand(SocketChannel channel) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ByteBuffer one = ByteBuffer.allocate(1);
            while (true) {
                one.clear();
                if (channel.read(one) < 0) throw new IOException("unexpected EOF");
                one.flip();
                byte value = one.get();
                if (value == 0) return bytes.toString(StandardCharsets.US_ASCII);
                bytes.write(value);
            }
        }

        private static byte[] readChunks(SocketChannel channel) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ByteBuffer length = ByteBuffer.allocate(4);
            while (true) {
                readFully(channel, length);
                length.flip();
                int size = length.getInt();
                length.clear();
                if (size == 0) return bytes.toByteArray();
                ByteBuffer chunk = ByteBuffer.allocate(size);
                readFully(channel, chunk);
                bytes.write(chunk.array());
            }
        }

        private static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) throw new IOException("unexpected EOF");
            }
        }

        private static void reply(SocketChannel channel, String reply) throws IOException {
            byte[] bytes = (reply + '\0').getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
        }

        @Override
        public void close() throws Exception {
            server.close();
            thread.join(1000);
            if (failure != null) throw failure;
        }
    }
}
