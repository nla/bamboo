package bamboo.cli;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.mizosoft.methanol.AdapterCodec;
import com.github.mizosoft.methanol.FormBodyPublisher;
import com.github.mizosoft.methanol.Methanol;
import com.github.mizosoft.methanol.MutableRequest;
import com.github.mizosoft.methanol.adapter.jackson.JacksonAdapterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;

import static java.nio.file.StandardOpenOption.*;

public class Auth {
    private static final Path SAVE_FILE = Paths.get(System.getProperty("user.home"), ".local", "bamboo", "auth");

    private final Methanol httpClient;
    private final JsonMapper mapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();

    private String configEndpoint = "https://login.nla.gov.au/new/realms/pandas/.well-known/openid-configuration";

    private State state = new State();
    private OpenidConfig openidConfig;

    static class State {
        public String clientId = "bamboo-cli";
        public String clientSecret;
        public String accessToken;
        public String refreshToken;
        public Instant expiry;
    }

    record OpenidConfig(
            URI tokenEndpoint,
            URI deviceAuthorizationEndpoint
    ) {
    }

    record DeviceAuthResponse(
            String deviceCode,
            URI verificationUriComplete,
            Integer interval) {
    }

    record TokenResponse(
            String accessToken,
            String tokenType,
            Integer expiresIn,
            String refreshToken,
            String scope
    ) implements TokenOrErrorResponse {
    }

    record ErrorResponse(String error) implements TokenOrErrorResponse {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
    @JsonSubTypes({
            @JsonSubTypes.Type(TokenResponse.class),
            @JsonSubTypes.Type(ErrorResponse.class)
    })
    sealed interface TokenOrErrorResponse {
    }

    public Auth(Methanol httpClient) {
        var openidMapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .addModule(new JavaTimeModule())
                .build();
        var codec = AdapterCodec.newBuilder()
                .encoder(JacksonAdapterFactory.createJsonEncoder(openidMapper))
                .decoder(JacksonAdapterFactory.createJsonDecoder(openidMapper))
                .build();
        this.httpClient = Methanol.newBuilder(httpClient).adapterCodec(codec).build();
    }

    public void deviceLogin() throws IOException, InterruptedException {
        var response = sendDeviceAuthRequest();
        System.out.println("Visit in browser: " + response.verificationUriComplete());
        int interval = response.interval() == null ? 5 : response.interval();
        while (true) {
            //noinspection BusyWait
            Thread.sleep(interval * 1000L);
            Instant pollTime = Instant.now();
            var requestBody = FormBodyPublisher.newBuilder()
                    .query("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
                    .query("device_code", response.deviceCode())
                    .query("client_id", state.clientId)
                    .build();
            var tokenOrErrorResponse = httpClient.send(MutableRequest.POST(openidConfig().tokenEndpoint(),
                    requestBody), TokenOrErrorResponse.class).body();
            if (tokenOrErrorResponse instanceof ErrorResponse error) {
                switch (error.error()) {
                    case "authorization_pending" -> {}
                    case "slow_down" -> interval += 5;
                    case "access_denied" -> {
                        System.err.println("Access denied");
                        System.exit(1);
                    }
                    case "expired_token" -> {
                        System.err.println("Device code expired");
                        System.exit(1);
                    }
                    default -> {
                        System.err.println("Unexpected error: " + error.error());
                        System.exit(1);
                    }
                }
            } else if (tokenOrErrorResponse instanceof TokenResponse token) {
                state.accessToken = token.accessToken();
                state.refreshToken = token.refreshToken();
                state.expiry = pollTime.plusSeconds(token.expiresIn());
                save();
                System.out.println("Logged in");
                break;
            } else {
                throw new IllegalStateException("Unexpected response type: " + tokenOrErrorResponse.getClass());
            }
        }
    }

    private DeviceAuthResponse sendDeviceAuthRequest() throws IOException, InterruptedException {
        var requestBody = FormBodyPublisher.newBuilder()
                .query("client_id", state.clientId)
                .query("scope", "openid")
                .build();
        var request = MutableRequest.POST(openidConfig().deviceAuthorizationEndpoint(), requestBody);
        return httpClient.send(request, DeviceAuthResponse.class).body();
    }

    private OpenidConfig openidConfig() throws InterruptedException {
        if (openidConfig == null) {
            try {
                this.openidConfig = httpClient.send(MutableRequest.GET(configEndpoint), OpenidConfig.class).body();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage() + " from " + openidConfig, e);
            }
        }
        return openidConfig;
    }

    private boolean isExpired() {
        int expirySlop = 30;
        return Instant.now().plusSeconds(expirySlop).isAfter(state.expiry);
    }

    public synchronized void login(String username, String password) throws IOException, InterruptedException {
        fetchToken(FormBodyPublisher.newBuilder()
                .query("grant_type", "password")
                .query("username", username)
                .query("password", password)
                .query("client_id", state.clientId)
                .query("scope", "openid")
                .build());
    }

    private void fetchToken(FormBodyPublisher body) throws IOException, InterruptedException {
        Instant now = Instant.now();
        var request = MutableRequest.POST(openidConfig().tokenEndpoint(), body);
        var response = httpClient.send(request, TokenOrErrorResponse.class).body();
        if (response instanceof ErrorResponse error) {
            throw new IOException("Error response: " + error.error());
        } else if (response instanceof TokenResponse data) {
            state.accessToken = data.accessToken();
            state.refreshToken = data.refreshToken();
            state.expiry = now.plusSeconds(data.expiresIn());
            save();
        } else {
            throw new IllegalStateException("Unexpected response type: " + response.getClass());
        }
    }

    public synchronized String bearer() throws IOException, InterruptedException {
        if (state.accessToken == null) throw new IllegalStateException("Not logged in");
        if (isExpired()) {
            fetchToken(FormBodyPublisher.newBuilder()
                    .query("grant_type", "refresh_token")
                    .query("refresh_token", state.refreshToken)
                    .query("client_id", state.clientId)
                    .build());
        }
        return "Bearer " + state.accessToken;
    }

    public void tryLoad() throws IOException {
        if (Files.exists(SAVE_FILE) && Files.size(SAVE_FILE) != 0) load();
    }

    public void load() throws IOException {
        state = mapper.readValue(SAVE_FILE.toFile(), State.class);
    }

    public void save() throws IOException {
        String data = mapper.writeValueAsString(state);
        if (!Files.exists(SAVE_FILE.getParent())) Files.createDirectories(SAVE_FILE.getParent());
        if (!Files.exists(SAVE_FILE)) Files.createFile(SAVE_FILE);
        Files.setPosixFilePermissions(SAVE_FILE, PosixFilePermissions.fromString("rw-------"));
        Files.writeString(SAVE_FILE, data, WRITE, TRUNCATE_EXISTING, CREATE);
    }
}
