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
import java.util.stream.Collectors;

public class BambooCLI {
    private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final Auth auth = new Auth(httpClient);
    private String baseUrl = System.getenv().getOrDefault("BAMBOO_URL", "https://pandas.nla.gov.au/bamboo");

    public static void main(String[] args) throws Exception {
        System.exit(new BambooCLI().run(args));
    }

    void usage() {
        System.err.println("Usage: bamboo subcommand [args]");
    }

    int run(String[] args) throws Exception {
        auth.tryLoad();
        switch (args[0]) {
            case "login":
                login(args.length > 1 ? args[1] : null, args.length > 2 ? args[2] : null, args.length > 3 ? args[3] : null);
                break;
            case "add-warc": {
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
                break;
            }
            case "delete-warc":{
                Long warcId = Long.parseLong(args[1]);
                var request = HttpRequest.newBuilder(URI.create(baseUrl + "/warcs/" + warcId))
                        .header("Authorization", auth.bearer())
                        .DELETE()
                        .build();
                var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                System.out.println(response.statusCode() + " " + response.body());
                break;
            }
            default:
                usage();
                return -1;
        }
        return 0;
    }

    void login(String url, String username, String password) throws IOException, InterruptedException, JsonParserException {
        if (url == null) url = System.console().readLine("Auth server URL: ");
        if (username == null) username = System.console().readLine("Username: ");
        if (password == null) password = new String(System.console().readPassword("Password: "));
        auth.login(url, username, password);
    }

}
