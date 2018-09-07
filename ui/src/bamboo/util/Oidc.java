package bamboo.util;

import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderConfigurationRequest;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;

import java.io.IOException;
import java.time.Instant;

/**
 * OpenID Connect client
 */
public class Oidc {
    private static final int TIME_SLOP_SECS = 10;
    private final Issuer authServer;
    private final ClientID clientId;
    private final Secret clientSecret;
    private OIDCProviderMetadata metadata;
    private AccessToken accessToken;
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
                    accessToken = refreshAccessToken();
                    accessTokenExpiry = now.plusSeconds(Math.min(0, accessToken.getLifetime() - TIME_SLOP_SECS));
                }
            }
        }
        return accessToken;
    }

    private AccessToken refreshAccessToken() throws IOException {
        try {
            TokenRequest request = new TokenRequest(metadata().getTokenEndpointURI(),
                    new ClientSecretBasic(clientId, clientSecret),
                    new ClientCredentialsGrant(),
                    new Scope("openid"));

            TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

            if (!response.indicatesSuccess()) {
                throw new RuntimeException("Auth error " + response.toErrorResponse().toJSONObject().toJSONString());
            }

            AccessTokenResponse successResponse = response.toSuccessResponse();
            return successResponse.getTokens().getAccessToken();
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
