package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageStorage
import com.tesco.aqueduct.pipe.api.OffsetEntity
import com.tesco.aqueduct.pipe.api.OffsetName
import com.tesco.aqueduct.pipe.api.PipeState
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET

class TimedMessageStorageSpec extends Specification {
    final long OFFSET = 1
    final Message MOCK_MESSAGE = Mock(Message)
    final List<String> MESSAGE_TYPES = []
    final SimpleMeterRegistry METER_REGISTRY = Spy(SimpleMeterRegistry)
    final String LOCATION_UUID = "locationUuid"

    def "read events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the read method"
        timedStorage.read(MESSAGE_TYPES, OFFSET, LOCATION_UUID)

        then: "the read method is called on the underlying storage"
        1 * mockedStorage.read(MESSAGE_TYPES, OFFSET, LOCATION_UUID)
    }

    def "read latest offsets are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the read method"
        timedStorage.getOffset(OffsetName.LOCAL_LATEST_OFFSET)

        then: "the read method is called on the underlying storage"
        1 * mockedStorage.getOffset(OffsetName.LOCAL_LATEST_OFFSET)
    }

    def "write message events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the write method"
        timedStorage.write(MOCK_MESSAGE)

        then: "the write method is called on the underlying storage"
        1 * mockedStorage.write(MOCK_MESSAGE)
    }

    def "write message list events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the write list method"
        timedStorage.write([MOCK_MESSAGE])

        then: "the write list method is called on the underlying storage"
        1 * mockedStorage.write([MOCK_MESSAGE])
    }

    def "get latest offset events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the get latest offset method"
        timedStorage.getLatestOffsetMatching(MESSAGE_TYPES)

        then: "the get latest method is called on the underlying storage"
        1 * mockedStorage.getLatestOffsetMatching(MESSAGE_TYPES)
    }

    def "write offset events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        and: "offset to write"
        def offset = new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(12))

        when: "we write the offset"
        timedStorage.write(offset)

        then: "the write offset is called on the underlying storage"
        1 * mockedStorage.write(offset)
    }

    def "write pipe state is timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        and: "pipe state to write"
        def pipeState = PipeState.UP_TO_DATE

        when: "we write the pipeState"
        timedStorage.write(pipeState)

        then: "the write pipeState is called on the underlying storage"
        1 * mockedStorage.write(pipeState)
    }
}
