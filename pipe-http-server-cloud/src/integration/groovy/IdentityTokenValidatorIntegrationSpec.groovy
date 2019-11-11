import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.pipe.http.PipeStateProvider
import com.tesco.aqueduct.pipe.storage.InMemoryStorage
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class IdentityTokenValidatorIntegrationSpec extends Specification {

    static final int CACHE_EXPIRY_SECONDS = 1
    static final String VALIDATE_TOKEN_BASE_PATH = '/v4/access-token/auth/validate'
    static final String USERNAME = "username"
    static final String PASSWORD = "password"
    static final String encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
    static InMemoryStorage storage = new InMemoryStorage(10, 600)

    static final String clientId = UUID.randomUUID().toString()
    static final String secret = UUID.randomUUID().toString()
    static final String userUID = UUID.randomUUID()

    static final String clientIdAndSecret = "trn:tesco:cid:${clientId}:${secret}"
    static final String clientUserUID = "trn:tesco:uid:uuid:${userUID}"
    static final String validateTokenPath = "${VALIDATE_TOKEN_BASE_PATH}?client_id=${clientIdAndSecret}"

    @Shared @AutoCleanup ErsatzServer identityMock
    @Shared @AutoCleanup ApplicationContext context

    def setupSpec() {
        identityMock = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        identityMock.start()

        context = ApplicationContext
            .build()
            .mainClass(EmbeddedServer)
            .properties(
                parseYamlConfig(
                    """
                micronaut.security.enabled: true
                micronaut.security.token.jwt.enabled: true
                micronaut.security.token.jwt.bearer.enabled: true
                micronaut.caches.identity-cache.expire-after-write: ${CACHE_EXPIRY_SECONDS}s
                identity.url: ${identityMock.getHttpUrl()}
                identity.validate.token.path: $validateTokenPath
                authentication.identity.clientId: $clientUserUID
                authentication:
                  users:
                    $USERNAME:
                      password: $PASSWORD
                """
                )
            )
            .build()

        context
            .registerSingleton(MessageReader, storage, Qualifiers.byName("local"))
            .registerSingleton(Mock(PipeStateProvider))
            .start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    def setup() {
        identityMock.clearExpectations()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'Http status OK when using a valid identity token'() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUID)

        when: 'A secured URL is accessed with the identity token as Bearer'
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.OK.code)

        then: 'identity was called'
        identityMock.verify()
    }

    def 'Http status OK when using valid basic auth credentials'() {
        expect: 'A secured URL is accessed with the basic auth'
        RestAssured.given()
            .header("Authorization", "Basic $encodedCredentials")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    def "Client receives unauthorised if no identity token provided."() {
        expect: 'Accessing a secured URL without authenticating'
        RestAssured.given()
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def 'Returns unauthorised when using an invalid identity token'() {
        given: 'Identity denies a validateToken request'
        denySingleIdentityTokenValidationRequest()

        when: 'A secured URL is accessed with the identity token as Bearer'
        def identityToken = UUID.randomUUID().toString()
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)

        then: 'identity was called'
        identityMock.verify()
    }

    def 'Returns unauthorised when using incorrect basic auth credentials'() {
        when: 'Identity denies a validateToken request'
        denySingleIdentityTokenValidationRequest()

        then: 'A secured .URL is accessed with the basic auth'
        def incorrectEncodedCredentials = "incorrectUser:incorrectPassword".bytes.encodeBase64().toString()

        RestAssured.given()
            .header("Authorization", "Basic $incorrectEncodedCredentials")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def "Token validation requests are cached for the duration specified in the config"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUID)

        when: 'A secured URL is multiple times with the identity token as Bearer'
        makeValidRequest(identityToken)
        makeValidRequest(identityToken)
        makeValidRequest(identityToken)

        then: 'Identity is only called once'
        identityMock.verify()

        when: 'Identity is called after the expiry period'
        identityMock.clearExpectations()
        sleep CACHE_EXPIRY_SECONDS * 1000

        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUID)
        makeValidRequest(identityToken)

        then: 'Identity is called again'
        identityMock.verify()
    }

    @Unroll
    def "A #valid identity token is used to call pipe"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUID)

        when: 'A secured URL is accessed with the identity token as Bearer'
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0")
            .then()
            .statusCode(statusCode)

        then: 'identity was called'
        identityMock.verify()

        where:
        valid             | clientUID      | statusCode
        "whitelisted"     | clientUserUID  | HttpStatus.OK.code
        "non whitelisted" | "incorrectUID" | HttpStatus.UNAUTHORIZED.code
    }

    def acceptSingleIdentityTokenValidationRequest(String clientIdAndSecret, String identityToken, String clientUserUID) {
        def json = JsonOutput.toJson([access_token: identityToken])

        identityMock.expectations {
            post(VALIDATE_TOKEN_BASE_PATH) {
                queries("client_id": [clientIdAndSecret])
                body(json, "application/json")
                called(1)

                responder {
                    header("Content-Type", "application/json;charset=UTF-8")
                    body("""
                        {
                          "UserId": "${clientUserUID}",
                          "Status": "VALID",
                          "Claims": [
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/clientid",
                              "value": "trn:tesco:cid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/scope",
                              "value": "oob"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/userkey",
                              "value": "trn:tesco:uid:uuid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/confidencelevel",
                              "value": "12"
                            },
                            {
                              "claimType": "http://schemas.microsoft.com/ws/2008/06/identity/claims/expiration",
                              "value": "1548413702"
                            }
                          ]
                        }
                    """)
                }
            }
        }
    }

    def denySingleIdentityTokenValidationRequest() {
        identityMock.expectations {
            post(VALIDATE_TOKEN_BASE_PATH) {
                called(1)
                responder {
                    header("Content-Type", "application/json;charset=UTF-8")
                    body("""
                        {
                          "Status": "INVALID"
                        }
                    """)
                }
            }
        }
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }

    def makeValidRequest(String identityToken) {
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    def acceptSingleIdentityTokenValidationRequestPerToken(String clientIdAndSecret, String... identityTokens) {
        identityTokens.each { token ->
            acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, token)
        }
    }
}
