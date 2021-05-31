package bamboo.cli;

import com.github.mizosoft.methanol.MediaType;
import com.github.mizosoft.methanol.MultipartBodyPublisher;
import com.github.mizosoft.methanol.ProgressTracker;
import com.grack.nanojson.JsonParserException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class BambooCLI {
    private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final Auth auth = new Auth(httpClient);
    private String baseUrl = System.getenv().getOrDefault("BAMBOO_URL", "https://pandas.nla.gov.au/bamboo");

    public static void main(String[] args) throws Exception {
        System.exit(new BambooCLI().run(args));
    }

    void usage() {
        System.err.println("Usage: bamboo subcommand [args]\n" +
                "\n" +
                "Subcommands:\n" +
                "  login <auth-url> <user>        Login to Bamboo\n" +
                "  add-warc <crawl-id> <files..>  Upload WARC files to an existing crawl\n" +
                "  delete-warc <warc-id>          Mark a WARC file as deleted and remove it from the CDX index\n");
    }

    int run(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(0);
        }
        auth.tryLoad();
        switch (args[0]) {
            case "login":
                login(args.length > 1 ? args[1] : null, args.length > 2 ? args[2] : null);
                break;
            case "add-warc":
                addWarc(args);
                break;
            case "delete-warc":
                deleteWarc(args[1]);
                break;
            default:
                usage();
                return -1;
        }
        return 0;
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

    void login(String username, String password) throws IOException, InterruptedException, JsonParserException {
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
        auth.login(url, username, password);
    }
}
