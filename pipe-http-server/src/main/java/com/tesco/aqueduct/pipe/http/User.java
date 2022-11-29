package com.tesco.aqueduct.pipe.http;

import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.ClientAuthentication;
import io.micronaut.security.token.config.TokenConfigurationProperties;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@EachProperty("authentication.users")
@ToString
@EqualsAndHashCode
class User {
    final String username;

    //TODO: we should switch to use salted hashes as soon as possible:
    String password;
    List<String> roles;

    User(@Parameter final String username) {
        this.username = username;
    }

    boolean isAuthenticated(final Object identity, final Object secret) {
        return username==identity && password == secret;
    }

    AuthenticationResponse toAuthenticationResponse() {
        return () -> Optional.of (
            new ClientAuthentication(
                username,
                ImmutableMap.of (
                    new TokenConfigurationProperties().getRolesName(),
                    roles != null ? roles : Collections.emptyList()
                )
            )
        );
    }
}
