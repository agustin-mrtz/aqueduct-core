package com.tesco.aqueduct.registry.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.IdentityToken
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.Node
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.reactivex.Single
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZonedDateTime

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class RegistryClientIntegrationSpec extends Specification {

    String HOST_1 = "http://host1"
    String HOST_2 = "http://host2"

    @Shared
    @AutoCleanup
    ErsatzServer server = new ErsatzServer()
    ApplicationContext context

    RegistryClient client

    def setupSpec() {
        server.start()
    }

    def cleanupSpec() {
        server.stop()
    }

    void setup() {
        context = ApplicationContext
            .builder()
            .properties(
                "registry.http.client.url": server.getHttpUrl() + "/v2",
                "registry.http.client.delay": "500ms",
                "registry.http.client.attempts": "2",
                "registry.http.client.reset": "15m",
                "registry.http.client.maxDelay": "15m",
                "registry.http.client.multiplier": "2",
            )
            .build()
            .registerSingleton(tokenProvider)
            .start()

        client = context.getBean(RegistryClient)
    }

    def "should register with node"() {
        given: "a node"
        def node = Node.builder()
            .group("1234")
            .localUrl(new URL("http://localhost"))
            .offset(0)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        and: "a valid response from the server"
        server.expectations {
            POST("/v2/registry") {
                header("Accept-Encoding", "gzip, deflate")
                responder {
                    contentType("application/json")

                    body("""{"requestedToFollow" : [ "$HOST_1", "$HOST_2" ], "bootstrapType" : "NONE"}""")
                }
            }
        }

        when: "we call register with a node"
        def response = client.registerAndConsumeBootstrapRequest(node)

        then: "we receive the correct response"
        response.requestedToFollow == [new URL(HOST_1), new URL(HOST_2)]
        response.bootstrapType == BootstrapType.NONE
    }

    def "Register request failed with 5XXX errors after some retries and delay"() {
        given: "a node"
        def node = Node.builder()
                .group("1234")
                .localUrl(new URL("http://localhost"))
                .offset(0)
                .status(INITIALISING)
                .lastSeen(ZonedDateTime.now())
                .build()

        and: "a error response from the server"
        server.clearExpectations()
        server.expectations {
            POST("/v2/registry") {
                header("Accept-Encoding", "gzip, deflate")
                called(3)
                responder {
                    code(500)
                }
            }
        }

        when: "we call register with a node"
        client.registerAndConsumeBootstrapRequest(node)

        then: "Register request is invoked and retry connection as per configured times and delay"
        server.verify()

        and:
        thrown(HttpClientResponseException)
    }

    def tokenProvider = Mock(TokenProvider) {
        retrieveIdentityToken() >> Single.just(Mock(IdentityToken))
    }

}

