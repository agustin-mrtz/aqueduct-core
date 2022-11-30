package com.tesco.aqueduct.pipe.identity.validator;

import com.google.common.collect.ImmutableMap;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityServiceUnavailableException;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.annotation.SingleResult;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.ClientAuthentication;
import io.micronaut.security.token.config.TokenConfigurationProperties;
import io.micronaut.security.token.validator.TokenValidator;
import io.reactivex.Flowable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Singleton
@Requires(property = "authentication.identity.url")
@Requires(property = "authentication.identity.users")
public class IdentityTokenValidator implements TokenValidator {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(IdentityTokenValidator.class));
    private final List<TokenUser> users;
    private final String clientIdAndSecret;
    private final IdentityTokenValidatorClient identityTokenValidatorClient;
    private final TokenConfigurationProperties tokenConfigurationProperties;

    @Inject
    public IdentityTokenValidator(
            final IdentityTokenValidatorClient identityTokenValidatorClient,
            @Value("${authentication.identity.client.id}") final String clientId,
            @Value("${authentication.identity.client.secret}") final String clientSecret,
            final List<TokenUser> users
    ) {
        this.identityTokenValidatorClient = identityTokenValidatorClient;
        this.clientIdAndSecret = clientId + ":" + clientSecret;
        this.users = users;
        this.tokenConfigurationProperties = new TokenConfigurationProperties();
    }

    @Override
    @SingleResult
    @Cacheable(value="identity-cache", parameters = "token")
    public Publisher<Authentication> validateToken(String token, @Nullable HttpRequest<?> request) {

        if (token == null) {
            LOG.error("token validator", "null token", "");
            return Flowable.empty();
        }

        return identityTokenValidatorClient
            .validateToken(traceId(), new ValidateTokenRequest(token), clientIdAndSecret)
            .doOnError(this::handleError)
            .filter(ValidateTokenResponse::isTokenValid)
            .map(ValidateTokenResponse::getClientUserID)
            .map(this::toUserAuthentication);
    }

    private void handleError(Throwable error) {
        if (error instanceof HttpClientResponseException) {
            HttpClientResponseException exception = (HttpClientResponseException) error;
            LOG.error("validateToken", "Identity response error", exception);
            if (exception.getStatus().getCode() > 499) {
                throw new IdentityServiceUnavailableException("Unexpected error from Identity with status - " + exception.getStatus());
            }
        } else {
            LOG.error("validateToken", "Unexpected error from Identity", error);
        }
    }

    private String traceId() {
        return MDC.get("trace_id") == null ? UUID.randomUUID().toString() : MDC.get("trace_id");
    }

    private Authentication toUserAuthentication(String clientId) {
        List<String> roles = users.stream()
            .filter(u -> Objects.equals(u.clientId, clientId))
            .filter(u -> u.roles != null)
            .map(u -> u.roles)
            .findFirst()
            .orElse(Collections.emptyList());
        return new ClientAuthentication(clientId, ImmutableMap.of(tokenConfigurationProperties.getRolesName(), roles));
    }

    //lowest precedence chosen so it is used after others
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
