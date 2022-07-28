package com.tesco.aqueduct.registry.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.IdentityToken
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.registry.model.Node
import io.micronaut.context.ApplicationContext
import io.reactivex.Single
import spock.lang.Specification

import java.time.ZonedDateTime

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class AuthenticateNodeRegistryFilterIntegrationSpec extends Specification {

    private static final URL MY_HOST = new URL("http://localhost")
    String host1 = "http://host1"
    String host2 = "http://host2"

    def identityToken= Mock(IdentityToken) {
        getAccessToken() >> "someToken"
    }
    def tokenProvider = Mock(TokenProvider) {
        retrieveIdentityToken() >> Single.just(identityToken)
    }

    def "Bearer auth is used for authorization"() {
        given: "Server expecting auth"

        def server = new ErsatzServer({
            expectations {
                POST("/v2/registry") {
                    called(1)

                    header("Authorization", "Bearer someToken")

                    responder {
                        contentType('application/json')
                        body("""{"requestedToFollow" : [ "$host1", "$host2" ], "bootstrapType" : "NONE"}""")
                    }
                }
            }
        })

        and: "client configured with auth"
        def context = ApplicationContext
            .builder()
            .properties(
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.delay": "500ms",
                "registry.http.client.attempts": "1",
                "registry.http.client.max-delay": "1m",
                "registry.http.client.multiplier": "2",
                "registry.http.client.reset": "1s",
                "registry.http.client.url": server.getHttpUrl() + "/v2",
            )
            .build()
            .registerSingleton(tokenProvider)
            .start()

        def client = context.getBean(RegistryClient)

        and: "a node to register"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        when: "the node is registered"
        client.registerAndConsumeBootstrapRequest(myNode)

        then: "the server receives the auth"
        server.verify()
    }

    def 'the base path of the client is respected when authenticating using a http client filter'() {
        given: "a base path exists"
        def basePath = "/messaging"

        def server = new ErsatzServer({
            expectations {
                POST("/messaging/v2/registry") {
                    header("Accept-Encoding", "gzip, deflate")
                    header("Authorization", "Bearer someToken")
                    called(1)
                    responder {
                        contentType("application/json")
                        body("""{"requestedToFollow" : [ "$host1", "$host2" ], "bootstrapType" : "NONE"}""")
                    }
                }
            }
        })

        and: 'an application context is created'
        def context = ApplicationContext
            .builder()
            .properties(
                "pipe.http.client.url": server.getHttpUrl() + basePath,
                "registry.http.client.url": server.getHttpUrl() + basePath + "/v2",
                "registry.http.client.delay": "500ms",
                "registry.http.client.attempts": "1",
                "registry.http.client.max-delay": "1m",
                "registry.http.client.multiplier": "2",
                "registry.http.client.reset": "1s",
            )
            .build()
            .registerSingleton(tokenProvider)
            .start()

        when: "calling the registry endpoint with a given node"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        def client = context.getBean(RegistryClient)
        def response = client.registerAndConsumeBootstrapRequest(myNode)

        then: "a list of expected urls are returned"
        response.requestedToFollow == [new URL(host1), new URL(host2)]

        and: "an exception is not raised"
        noExceptionThrown()
    }
}
