package bamboo.cli;

import com.grack.nanojson.JsonObject;
import com.grack.nanojson.JsonParser;
import com.grack.nanojson.JsonParserException;
import com.grack.nanojson.JsonWriter;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

public class Auth {
    private static Path saveFile = Paths.get(System.getProperty("user.home"), ".local", "bamboo", "auth");

    private final HttpClient httpClient;

    private int expirySlop = 30;

    private String url;
    private String clientId = "bamboo-cli";
    private String clientSecret;
    private String accessToken;
    private String refreshToken;
    private Instant expiry;

    public Auth(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    private String clientCredentials() {
        StringBuilder sb = new StringBuilder();
        if (clientId != null) sb.append("&client_id=").append(URLEncoder.encode(clientId, UTF_8));
        if (clientSecret != null) sb.append("&client_secret=").append(URLEncoder.encode(clientSecret, UTF_8));
        return sb.toString();
    }

    private boolean isExpired() {
        return Instant.now().plusSeconds(expirySlop).isAfter(expiry);
    }

    public synchronized void login(String url, String username, String password) throws IOException, InterruptedException {
        this.url = url;
        fetchToken("grant_type=password&username=" + URLEncoder.encode(username, UTF_8) +
                "&password=" + URLEncoder.encode(password, UTF_8) + clientCredentials());
    }

    private void fetchToken(String body) throws IOException, InterruptedException {
        Instant now = Instant.now();
        var request = HttpRequest.newBuilder(URI.create(url + "/protocol/openid-connect/token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) throw new IOException("Auth error: " + response.statusCode() + ": " + response.body());
        JsonObject data;
        try {
            data = JsonParser.object().from(response.body());
        } catch (JsonParserException e) {
            throw new IOException("Invalid json response from auth server", e);
        }
        accessToken = data.getString("access_token");
        refreshToken = data.getString("refresh_token");
        long expiresIn = data.getLong("expires_in");
        expiry = now.plusSeconds(expiresIn);
        save();
    }

    public synchronized String bearer() throws IOException, InterruptedException {
        if (accessToken == null) throw new IllegalStateException("Not logged in");
        if (isExpired()) {
            refresh();
        }
        return "Bearer " + accessToken;
    }

    void refresh() throws IOException, InterruptedException {
        fetchToken("grant_type=refresh_token&refresh_token=" + URLEncoder.encode(refreshToken, UTF_8) + clientCredentials());
    }

    public String toJson() {
        return JsonWriter.string().object()
                .value("url", url)
                .value("accessToken", accessToken)
                .value("refreshToken", refreshToken)
                .value("expiry", expiry == null ? null : expiry.toString())
                .end()
                .done();
    }

    public void tryLoad() throws IOException, JsonParserException {
        if (Files.exists(saveFile)) load();
    }

    public void load() throws IOException, JsonParserException {
        JsonObject data = JsonParser.object().from(Files.readString(saveFile));
        url = data.getString("url");
        accessToken = data.getString("accessToken");
        refreshToken = data.getString("refreshToken");
        String expiryStr = data.getString("expiry");
        this.expiry = expiryStr == null ? null : Instant.parse(expiryStr);
    }

    public void save() throws IOException {
        if (!Files.exists(saveFile.getParent())) Files.createDirectories(saveFile.getParent());
        if (!Files.exists(saveFile)) Files.createFile(saveFile);
        Files.setPosixFilePermissions(saveFile, PosixFilePermissions.fromString("rw-------"));
        Files.writeString(saveFile, toJson(), WRITE, TRUNCATE_EXISTING, CREATE);
    }
}
