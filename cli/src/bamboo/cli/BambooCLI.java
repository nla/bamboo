package bamboo.cli;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.mizosoft.methanol.*;
import com.github.mizosoft.methanol.adapter.jackson.JacksonAdapterFactory;
import com.grack.nanojson.JsonParser;
import com.grack.nanojson.JsonParserException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

public class BambooCLI {
    private final JsonMapper mapper = new JsonMapper();
    private final Methanol httpClient = Methanol.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .adapterCodec(AdapterCodec.newBuilder()
                    .encoder(JacksonAdapterFactory.createJsonEncoder(mapper))
                    .decoder(JacksonAdapterFactory.createJsonDecoder(mapper))
                    .build())
            .build();
    private final Auth auth = new Auth(httpClient);
    private String baseUrl = System.getenv().getOrDefault("BAMBOO_URL", "https://pandas.nla.gov.au/bamboo");

    public static void main(String[] args) throws Exception {
        System.exit(new BambooCLI().run(args));
    }

    void usage() {
        System.err.println("Usage: bamboo subcommand [args]\n" +
                "\n" +
                "Subcommands:\n" +
                "  login                              Login to Bamboo via browser\n" +
                "  password-login <user>              Login to Bamboo via username/password\n" +
                "  add-artifact <crawl-id> <files..>  Upload artifacts to an existing crawl\n" +
                "  add-warc <crawl-id> <files..>      Upload WARC files to an existing crawl\n" +
                "  delete-warc <warc-id>              Mark a WARC file as deleted and remove it from the CDX index\n");
    }

    int run(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(0);
        }
        auth.tryLoad();
        switch (args[0]) {
            case "add-artifact":
                addArtifact(args);
                break;
            case "add-warc":
                addWarc(args);
                break;
            case "delete-warc":
                deleteWarc(args[1]);
                break;
            case "password-login":
                passwordLogin(args.length > 1 ? args[1] : null, args.length > 2 ? args[2] : null);
                break;
            case "login":
                auth.deviceLogin();
                break;
            default:
                usage();
                return -1;
        }
        return 0;
    }

    private void addArtifact(String[] args) throws IOException, InterruptedException {
        long crawlId = Long.parseLong(args[1]);
        for (int i = 2; i < args.length; i++) {
            Path path = Paths.get(args[i]);
            long size = Files.size(path);
            try (InputStream inputStream = Files.newInputStream(path)) {
                var connection = (HttpURLConnection) new URL(baseUrl + "/crawls/" + crawlId + "/artifacts/" + path).openConnection();
                connection.setInstanceFollowRedirects(false);
                connection.setRequestMethod("PUT");
                connection.setFixedLengthStreamingMode(size);
                connection.addRequestProperty("Authorization", auth.bearer());
                connection.setDoOutput(true);
                try (OutputStream outputStream = connection.getOutputStream()) {
                    long written = inputStream.transferTo(outputStream);
                    if (written != size) {
                        connection.disconnect();
                        throw new IOException("wrote " + written + " bytes but expected to write " + size);
                    }
                }
                int status = connection.getResponseCode();
                if (status != 201) {
                    throw new IOException("expected status 201 but got status " + status);
                }
            }
        }
    }

    private void addWarc(String[] args) throws IOException, InterruptedException {
        long crawlId = Long.parseLong(args[1]);
        var warcFiles = Arrays.stream(args).skip(2).map(Paths::get).collect(Collectors.toList());
        for (Path file : warcFiles) {
            MultipartBodyPublisher body = MultipartBodyPublisher.newBuilder().filePart("warcFile", file, MediaType.parse("application/warc")).build();
            ProgressTracker tracker = ProgressTracker.newBuilder().timePassedThreshold(Duration.ofSeconds(1)).build();
            var request = HttpRequest.newBuilder(URI.create(baseUrl + "/crawls/" + crawlId + "/warcs/upload"))
                    .header("Content-Type", body.mediaType().toString())
                    .header("Authorization", auth.bearer())
                    .POST(tracker.trackingMultipart(body, item -> System.out.println(file.getFileName() + " " + item.partProgress())))
                    .build();
            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.statusCode() + " " + response.body() + " " + response.headers().firstValue("Location").orElse(""));
        }
    }

    private void deleteWarc(String arg) throws IOException, InterruptedException {
        long warcId = Long.parseLong(arg);
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/warcs/" + warcId))
                .header("Authorization", auth.bearer())
                .DELETE()
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response.statusCode() + " " + response.body());
    }

    void passwordLogin(String username, String password) throws IOException, InterruptedException, JsonParserException {
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/oauth2/authorization/oidc")).GET().build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            System.err.println("Server returned 200 OK. No login appears to be required?");
            System.exit(1);
        } else if (response.statusCode() != 302) {
            System.err.println("Unexpected repsonse code: " + response.statusCode());
            System.exit(1);
        }
        String location = response.headers().firstValue("Location").orElseThrow();
        String url = location.replaceFirst("/protocol/openid-connect/auth\\?.*", "");
        System.out.println("Authenticating to realm " + url);
        if (username == null) username = System.console().readLine("Username: ");
        if (password == null) password = new String(System.console().readPassword("Password: "));
        auth.login(username, password);
    }

    private void login2() throws IOException, InterruptedException, JsonParserException {
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/oauth2/auth/device"))
                .POST(BodyPublishers.ofString("clientId=bamboo-cli&scope=openid"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            System.err.println("Unexpected response code: " + response.statusCode() + " from " + request.uri());
            System.exit(1);
        }
        var message = JsonParser.object().from(response.body());
        System.out.println("Visit in browser: " + message.getString("verification_uri_complete"));
        int interval = message.getInt("interval", 5);
        while (true) {
            Thread.sleep(interval * 1000);
            var request2 = HttpRequest.newBuilder(URI.create(baseUrl + "/oauth2/auth/device"));

        }
    }


}
