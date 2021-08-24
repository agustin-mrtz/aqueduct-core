import Helper.SqlWrapper
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.HttpHeaders
import com.tesco.aqueduct.pipe.storage.ClusterCacheEntry
import com.tesco.aqueduct.pipe.storage.ClusterStorage
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.time.LocalDateTime

import static org.hamcrest.Matchers.equalTo

class PipeCloudServerIntegrationSpec extends Specification {
    static LocalDateTime time = LocalDateTime.parse("2018-12-20T15:13:01")

    // Starts real PostgreSQL database, takes some time to create it and clean it up.
    @Shared @ClassRule SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()
    @AutoCleanup Sql sql
    @AutoCleanup("stop") ApplicationContext context

    DataSource dataSource
    ClusterStorage clusterStorage

    def setup() {

        sql = new SqlWrapper(pg.embeddedPostgres.postgresDatabase).sql

        dataSource = Mock()
        clusterStorage = Mock()

        dataSource.connection >>> [
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection,
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection,
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection,
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection,
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection
        ]

        context = ApplicationContext
            .build()
            .properties(
                "pipe.server.url": "http://cloud.pipe",
                "persistence.read.limit": 1000,
                "persistence.read.retry-after": 10000,
                "persistence.read.max-batch-size": "10485760",
                "persistence.read.expected-node-count": 2,
                "persistence.read.cluster-db-pool-size": 10,
                "micronaut.security.enabled": "false",
                "compression.threshold-in-bytes": 1024,
                "micronaut.caches.latest-offset-cache.expire-after-write": "10s",
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, dataSource, Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, dataSource, Qualifiers.byName("registry"))
            .registerSingleton(DataSource, dataSource, Qualifiers.byName("compaction"))
            .registerSingleton(ClusterStorage, clusterStorage)

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    def cleanup() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "once I inserted some documents in database I can read them from the pipe" () {
        given:
        insertMessage(100,  "a", "contentType", "type1", time, "data")
        insertMessage(101, "b", "contentType", "type1", time, null)

        and: "location to cluster resolution"
        clusterStorage.getClusterCacheEntry("someLocation", _ as Connection) >> clusterCacheEntry("someLocation", [1L])

        when:
        def request = RestAssured.get("/pipe/100?location=someLocation")

        then:
        request
            .then()
            // this is bit fragile on purpose, it will fail on small changes to format of Json
            .body(equalTo("""
            [
                {"type":"type1","key":"a","contentType":"contentType","offset":"100","created":"2018-12-20T15:13:01Z","data":"data"},
                {"type":"type1","key":"b","contentType":"contentType","offset":"101","created":"2018-12-20T15:13:01Z"}
            ]
            """.replaceAll("\\s", "")))
    }

    def "the global latest offset is cached" () {
        given:
        insertMessage(100,  "a", "contentType", "type1", time, "data")
        insertMessage(101, "b", "contentType", "type1", time, null)

        and: "location to cluster resolution"
        clusterStorage.getClusterCacheEntry("someLocation", _ as Connection) >> clusterCacheEntry("someLocation", [1L])

        when:
        def request1 = RestAssured.get("/pipe/100?location=someLocation")

        then:
        request1
            .then()
            .header(HttpHeaders.GLOBAL_LATEST_OFFSET.toString(), equalTo("101"))

        when: "more data is inserted"
        insertMessage(102, "b", "contentType", "type1", time, null)
        insertMessage(103, "b", "contentType", "type1", time, null)

        and:
        def request2 = RestAssured.get("/pipe/100?location=someLocation")

        then:
        request2
            .then()
            .header(HttpHeaders.GLOBAL_LATEST_OFFSET.toString(), equalTo("101"))
    }

    Optional<ClusterCacheEntry> clusterCacheEntry(String locationUuid, List<Long> clusterIds) {
        Optional.of(new ClusterCacheEntry(locationUuid, clusterIds, LocalDateTime.now().plusMinutes(1), true))
    }

    void insertMessage(Long msg_offset, String msg_key, String content_type, String type, LocalDateTime created, String data) {
        sql.execute(
            "INSERT INTO EVENTS (msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?,?);" +
            "INSERT INTO OFFSETS (name, value) VALUES ('global_latest_offset', ?) ON CONFLICT(name) DO UPDATE SET VALUE = ?;",
            msg_offset, msg_key, content_type, type, created, data, 0, msg_offset, msg_offset
        )
    }


}