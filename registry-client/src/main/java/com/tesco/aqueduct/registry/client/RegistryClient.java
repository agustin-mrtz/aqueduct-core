package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.RegistryResponse;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;

@Client("${registry.http.client.url}")
public interface RegistryClient {
    @CircuitBreaker(delay = "${registry.http.client.delay}",
            maxDelay = "${registry.http.client.max-delay}",
            multiplier = "${registry.http.client.multiplier}",
            attempts = "${registry.http.client.attempts}",
            reset = "${registry.http.client.reset}")
    @Post(uri = "/registry")
    @Header(name="Accept-Encoding", value="gzip, deflate")
    RegistryResponse registerAndConsumeBootstrapRequest(@Body Node node);
}
