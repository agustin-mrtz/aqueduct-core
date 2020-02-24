package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageResults
import com.tesco.aqueduct.pipe.api.OffsetName
import com.tesco.aqueduct.pipe.api.PipeState
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.ZonedDateTime

class PostgresqlStorageIntegrationSpec extends StorageSpec {

    // Starts real PostgreSQL database, takes some time to create it and clean it up.
    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    PostgresqlStorage storage
    DataSource dataSource

    private static final long CLUSTER_A = 1L
    private static final long CLUSTER_B = 2L
    long retryAfter = 5000
    long batchSize = 1000
    long maxOverheadBatchSize = (Message.MAX_OVERHEAD_SIZE * limit) + batchSize

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
          
            CREATE TABLE EVENTS(
                msg_offset bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY NOT NULL,
                msg_key varchar NOT NULL, 
                content_type varchar NOT NULL, 
                type varchar NOT NULL, 
                created_utc timestamp NOT NULL,
                tags JSONB NULL, 
                data text NULL,
                event_size int NOT NULL,
                cluster_id BIGINT NOT NULL DEFAULT 1
            );
        """)

        storage = new PostgresqlStorage(dataSource, limit, retryAfter, batchSize)
    }

    def "get the latest offset where tags contains type but no other keys"() {
        given: "there is postgres storage"
        def dataSourceWithMockedConnection = Mock(DataSource)
        def postgresStorage = new PostgresqlStorage(dataSourceWithMockedConnection, limit, 0, batchSize)

        and: "a mock connection is provided when requested"
        def connection = Mock(Connection)
        dataSourceWithMockedConnection.getConnection() >> connection

        and: "a connection returns a prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        preparedStatement.executeQuery() >> Mock(ResultSet)
        connection.prepareStatement(_ as String) >> preparedStatement

        when: "requesting the latest matching offset with tags specifying a type key"
        postgresStorage.getLatestOffsetMatching(["some_type"])

        then: "a query is created that does not contain tags in the where clause"
        1 * preparedStatement.setString(1, "some_type")
        0 * preparedStatement.setString(_ as Integer, '{}')
    }

    @Unroll
    def "get #offsetName returns max offset"() {
        given: "there are messages"
        def msg1 = message(offset: 1)
        def msg2 = message(offset: 2)
        def msg3 = message(offset: 3)

        and: "they are inserted into the integrated database"
        insert(msg1, 10)
        insert(msg2, 10)
        insert(msg3, 10)

        when: "reading latest offset from the database"
        def offset = storage.getOffset(offsetName)

        then: "offset should be the offset of latest message in the storage"
        offset.getAsLong() == 3

        where:
        offsetName                          | _
        OffsetName.GLOBAL_LATEST_OFFSET     | _
        OffsetName.NEXT_OFFSET              | _
        OffsetName.LOCAL_LATEST_OFFSET      | _
    }

    def "get pipe state as up to date always"() {
        when: "reading the messages"
        def messageResults = storage.read(["some_type"], 0, "someLocationUuid")

        then: "pipe state is up to date"
        messageResults.pipeState == PipeState.UP_TO_DATE
    }

    def "get messages where tags contains type but no other keys"() {
        given: "there is postgres storage"
        def limit = 1
        def dataSourceWithMockedConnection = Mock(DataSource)
        def postgresStorage = new PostgresqlStorage(dataSourceWithMockedConnection, limit, 0, batchSize)

        and: "a mock connection is provided when requested"
        def connection = Mock(Connection)
        dataSourceWithMockedConnection.getConnection() >> connection

        and: "a connection returns a prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        preparedStatement.executeQuery() >> Mock(ResultSet)
        connection.prepareStatement(_ as String) >> preparedStatement

        when: "requesting messages with tags specifying a type key"
        postgresStorage.read(["some_type"], 0, "locationUuid")

        then: "a query is created that does not contain tags in the where clause"
        0 * preparedStatement.setString(1, "some_type")
        0 * preparedStatement.setString(_ as Integer, '{}')
    }

    def "the messages returned are no larger than the maximum batch size when reading without a type"() {
        given: "there are messages with unique keys"
        def msg1 = message(key: "x")
        def msg2 = message(key: "y")
        def msg3 = message(key: "z")

        and: "the size of each message is set so that 3 messages are just larger than the max overhead batch size"
        int messageSize = Double.valueOf(maxOverheadBatchSize / 3).intValue() + 1

        and: "they are inserted into the integrated database"
        insert(msg1, messageSize)
        insert(msg2, messageSize)
        insert(msg3, messageSize)

        when: "reading from the database"
        MessageResults result = storage.read([], 0, "locationUuid")

        then: "messages that are returned are no larger than the maximum batch size when reading with a type"
        result.messages.size() == 2
    }

    def "the messages returned are no larger than the maximum batch size"() {
        given: "there are messages with unique keys"
        def msg1 = message(key: "x", type: "type-1")
        def msg2 = message(key: "y", type: "type-1")
        def msg3 = message(key: "z", type: "type-1")

        and: "the size of each message is set so that 3 messages are just larger than the max overhead batch size"
        int messageSize = Double.valueOf(maxOverheadBatchSize / 3).intValue() + 1

        and: "they are inserted into the integrated database"
        insert(msg1, messageSize)
        insert(msg2, messageSize)
        insert(msg3, messageSize)

        when: "reading from the database"
        MessageResults result = storage.read(["type-1"], 0, "locationUuid")

        then: "messages that are returned are no larger than the maximum batch size"
        result.messages.size() == 2
    }

    def "retry-after is zero if the pipe is not empty"() {
        given: "I have some records in the integrated database"
        insert(message(key: "z"))
        insert(message(key: "y"))
        insert(message(key: "x"))

        when:
        MessageResults result = storage.read([], 0, "locationUuid")

        then:
        result.retryAfterSeconds == 0
        result.messages.size() == 3
    }

    def "retry-after is non-zero if the pipe has no more data at specified offset"() {
        given: "I have some records in the integrated database"
        insert(message(key: "z"))
        insert(message(key: "y"))
        insert(message(key: "x"))

        when:
        MessageResults result = storage.read([], 4, "locationUuid")

        then:
        result.retryAfterSeconds > 0
        result.messages.isEmpty()
    }

    def "retry-after is non-zero if the pipe has no data"() {
        given: "I have no records in the integrated database"

        when:
        MessageResults result = storage.read([], 0,"locationUuid")

        then:
        result.retryAfterSeconds > 0
        result.messages.isEmpty()
    }

    def 'All duplicate messages are compacted for whole data store'() {
        given: 'an existing data store with duplicate messages for the same key'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(2, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(3, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)

        when: 'compaction is run on the whole data store'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults result = storage.read(null, 0, "locationUuid")
        List<Message> retrievedMessages = result.messages

        then: 'duplicate messages are deleted'
        retrievedMessages.size() == 2

        and: 'the correct compacted message list is returned in the message results'
        result.messages*.offset*.intValue() == [2, 3]
        result.messages*.key == ["B", "A"]
    }

    def 'Messages with the same key but different clusters are not compacted'() {
        given: 'an existing data store with 2 messages with same key but different clusters'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_B)

        when: 'compaction is run on the whole data store'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults result = storage.read(null, 0, "locationUuid")
        List<Message> retrievedMessages = result.messages

        then:
        retrievedMessages.size() == 2
        and: 'the correct compacted message list is returned in the message results'
        result.messages*.offset*.intValue() == [1, 2]
        result.messages*.key == ["A", "A"]
    }

    def 'Duplicate messages are not compacted when published after the threshold'() {
        given: 'an existing data store with duplicate messages for the same key'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(3, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(4, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), CLUSTER_A)

        when: 'compaction is run up to the timestamp of offset 1'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = storage.read(null, 1, "locationUuid")

        then: 'duplicate messages are not deleted as they are beyond the threshold'
        messageResults.messages.size() == 4
        messageResults.messages*.offset*.intValue() == [1, 2, 3, 4]
        messageResults.messages*.key == ["A", "A", "B", "A"]
    }

    def 'All duplicate messages are compacted to a given offset, complex case'() {
        given: 'an existing data store with duplicate messages for the same key'
        insert(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(4, "type", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))
        insert(message(6, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))
        insert(message(7, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))
        insert(message(8, "type", "D", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))

        when: 'compaction is run up to the timestamp of offset 4'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = storage.read(null, 1, "locationUuid")

        then: 'duplicate messages are deleted that are within the threshold'
        messageResults.messages.size() == 7
        messageResults.messages*.offset*.intValue() == [1, 2, 4, 5, 6, 7, 8]
        messageResults.messages*.key == ["A", "B", "C", "A", "B", "B", "D"]
    }

    def 'All duplicate messages are compacted to a given offset per cluster, complex case'() {
        given: 'an existing data store with duplicate messages for the same key'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(3, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_B)
        insertWithCluster(message(4, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_B)
        insertWithCluster(message(5, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(6, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), CLUSTER_B)
        insertWithCluster(message(7, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), CLUSTER_B)
        insertWithCluster(message(8, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), CLUSTER_A)
        insertWithCluster(message(9, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), CLUSTER_B)

        when: 'compaction is run up to the timestamp of offset 4'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = storage.read(null, 1, "locationUuid")

        then: 'duplicate messages are deleted that are within the threshold'
        messageResults.messages.size() == 7
        messageResults.messages*.offset*.intValue() == [2, 4, 5, 6, 7, 8, 9]
        messageResults.messages*.key == ["A", "A", "B", "B", "B", "A", "A"]
    }

    @Unroll
    def 'Global latest offset is returned'() {
        given: 'an existing data store with two different types of messages'
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading all messages'
        def messageResults = storage.read([type], 0, locationUuid)

        then: 'global latest offset is type and locationUuid independent'
        messageResults.globalLatestOffset == OptionalLong.of(3)

        where:
        type    | locationUuid
        "type1" | "locationUuid1"
        "type2" | "locationUuid2"
        "type3" | "locationUuid3"
    }

    @Unroll
    def 'pipe should always return messages if there any left for a given type'(){
        given: "there is postgres storage"
        def limit = 3
        storage = new PostgresqlStorage(dataSource, limit, retryAfter, batchSize)

        and: 'an existing data store with two different types of messages'
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type2", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(6, "type2", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(8, "type1", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(9, "type1", "I", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading all messages'
        def messageResults = storage.read(["type1"], 0, "some-location")

        then: 'duplicate messages are deleted that are within the threshold'
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
        messageResults.globalLatestOffset == OptionalLong.of(9)

        when:
        messageResults = storage.read(["type1"], 4, "some-location")

        then:
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["G", "H", "I"]
        messageResults.messages*.offset*.intValue() == [7, 8, 9]
        messageResults.globalLatestOffset == OptionalLong.of(9)
    }

    @Override
    void insert(Message msg, int maxMessageSize=0, def time = Timestamp.valueOf(msg.created.toLocalDateTime()) ) {

        if (msg.offset == null) {
            sql.execute(
                "INSERT INTO EVENTS(msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?);",
                msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize
            )
        } else {
            sql.execute(
                "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?,?);",
                msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize
            )
        }
    }

    void insertWithCluster(Message msg, Long clusterId, def time = Timestamp.valueOf(msg.created.toLocalDateTime()), int maxMessageSize=0) {
        sql.execute(
            "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
            msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize, clusterId
        )
    }
}
