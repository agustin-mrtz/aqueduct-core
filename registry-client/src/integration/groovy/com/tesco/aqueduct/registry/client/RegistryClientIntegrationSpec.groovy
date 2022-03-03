package com.tesco.aqueduct.registry.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.IdentityToken
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.registry.model.Bootstrapable
import com.tesco.aqueduct.registry.model.Node
import com.tesco.aqueduct.registry.model.Resetable
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.netty.DefaultHttpClient
import io.micronaut.inject.qualifiers.Qualifiers
import io.reactivex.Single
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZonedDateTime
import java.util.function.Supplier

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class RegistryClientIntegrationSpec extends Specification {

    private static final URL MY_HOST = new URL("http://localhost")
    String host1 = "http://host1"
    String host2 = "http://host2"

    def tokenProvider = Mock(TokenProvider) {
        retrieveIdentityToken() >> Single.just(Mock(IdentityToken))
    }

    @Shared @AutoCleanup ErsatzServer server

    SummarySupplier selfSummarySupplier = Mock()

    Supplier<Map<String, Object>> providerMetricsSupplier = Mock()
    def setupSpec() {
        server = new ErsatzServer()
        server.start()
    }

    def 'test registry endpoint'() {
        given: "A dummy server's application context"

        def context = ApplicationContext
            .builder()
            .properties(
                "registry.http.client.url": server.getHttpUrl() + "/v2",
                "registry.http.interval": "1m",
                "registry.http.client.delay": "500ms",
                "registry.http.client.attempts": "1",
                "registry.http.client.reset": "1s",
                "pipe.http.client.url": server.getHttpUrl(),
                "pipe.http.client.healthcheck.interval": "1m"
            )
            .build()
            .registerSingleton(tokenProvider)
            .registerSingleton(SummarySupplier.class, selfSummarySupplier, Qualifiers.byName("selfSummarySupplier"))
            .registerSingleton(Supplier.class, providerMetricsSupplier, Qualifiers.byName("providerMetricsSupplier"))
            .registerSingleton(Bootstrapable.class, Mock(Bootstrapable), Qualifiers.byName("provider"))
            .registerSingleton(Bootstrapable.class, Mock(Bootstrapable), Qualifiers.byName("pipe"))
            .registerSingleton(Bootstrapable.class, Mock(Bootstrapable), Qualifiers.byName("controller"))
            .registerSingleton(Resetable.class, Mock(Resetable), Qualifiers.byName("corruptionManager"))
            .registerSingleton(new ServiceList(
                new DefaultHttpClient(),
                new PipeServiceInstance(new DefaultHttpClient(), new URL(server.getHttpUrl())),
                File.createTempFile("provider", "properties")
            ))
            .start()

        and: "a fake response from the server"
        server.expectations {
            POST("/v2/registry") {
                header("Accept-Encoding", "gzip, deflate")
                called(1)

                responder {
                    contentType("application/json")
                    body("""{"requestedToFollow" : [ "$host1", "$host2" ], "bootstrapType" : "NONE"}""")
                }
            }
            GET("/pipe/_status") {
                called(1)
                responder {
                    contentType("application/json")
                }
            }
        }

        and: "A Micronaut-generated Client"
        def client = context.getBean(RegistryClient)

        and: "We have a node in the NodeRegistry"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        selfSummarySupplier.getSelfNode() >> {
            myNode
        }

        when: "We call register using the Micronaut client"
        def response = client.registerAndConsumeBootstrapRequest(myNode)

        then: "We expect the dummy server to return a list of URLs"
        response.requestedToFollow.size() == 2
        response.requestedToFollow == [new URL(host1), new URL(host2)]

        cleanup:
        server.stop()
    }
}

