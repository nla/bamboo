package bamboo.util;

import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderConfigurationRequest;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

/**
 * OpenID Connect client
 */
public class Oidc {
    private static final Logger log = LoggerFactory.getLogger(Oidc.class);
    private static final int TIME_SLOP_SECS = 10;
    private final Issuer authServer;
    private final ClientID clientId;
    private final Secret clientSecret;
    private OIDCProviderMetadata metadata;
    private Tokens tokens;
    private Instant accessTokenExpiry = Instant.MIN;

    public Oidc(String authServerUrl, String clientId, String clientSecret) {
        this.authServer = new Issuer(authServerUrl);
        this.clientId = new ClientID(clientId);
        this.clientSecret = new Secret(clientSecret);
    }

    public synchronized AccessToken accessToken() throws IOException {
        Instant now = Instant.now();
        if (now.isAfter(accessTokenExpiry)) {
            synchronized (this) {
                if (now.isAfter(accessTokenExpiry)) {
                    log.info("Refreshing OIDC service account access token");
                    tokens = refreshTokens();
                    accessTokenExpiry = now.plusSeconds(Math.max(0, tokens.getAccessToken().getLifetime() - TIME_SLOP_SECS));
                }
            }
        }
        return tokens.getAccessToken();
    }

    private Tokens refreshTokens() throws IOException {
        try {
            // if we have a refresh token try that first
            if (tokens != null && tokens.getRefreshToken() != null) {
                TokenRequest request = new TokenRequest(metadata().getTokenEndpointURI(),
                        new ClientSecretBasic(clientId, clientSecret),
                        new RefreshTokenGrant(tokens.getRefreshToken()));
                TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

                if (response.indicatesSuccess()) {
                    return response.toSuccessResponse().getTokens();
                }

                ErrorObject error = response.toErrorResponse().getErrorObject();
                log.info("OIDC service account token refresh failed. Reauthenticating. (" + error.getDescription() + ")");
            }


            TokenRequest request = new TokenRequest(metadata().getTokenEndpointURI(),
                    new ClientSecretBasic(clientId, clientSecret),
                    new ClientCredentialsGrant(),
                    new Scope("openid"));

            TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

            if (!response.indicatesSuccess()) {
                throw new RuntimeException("Auth error " + response.toErrorResponse().toJSONObject().toJSONString());
            }

            AccessTokenResponse successResponse = response.toSuccessResponse();
            return successResponse.getTokens();
        } catch (ParseException e) {
            throw new IOException("OIDC parse error", e);
        }
    }

    private synchronized OIDCProviderMetadata metadata() throws IOException, ParseException {
        if (metadata == null) {
            HTTPResponse response = new OIDCProviderConfigurationRequest(authServer).toHTTPRequest().send();
            metadata = OIDCProviderMetadata.parse(response.getContentAsJSONObject());
        }
        return metadata;
    }
}
