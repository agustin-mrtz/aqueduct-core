package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.OffsetName;

import java.util.stream.Collectors;
import java.util.stream.Stream;

final class SQLiteQueries {
    public static final String UPSERT_PIPE_STATE =
        "INSERT INTO PIPE_STATE (name, value) VALUES (?,?) " +
        "ON CONFLICT(name) DO UPDATE SET VALUE = ?;";

    public static final String GET_PIPE_STATE =
        "SELECT value FROM PIPE_STATE WHERE name='pipe_state';";

    public static final String REINDEX_EVENTS = "REINDEX EVENT;";

    public static final String DROP_TYPES_INDEX = "DROP INDEX IF EXISTS type_idx;";

    static final String CREATE_EVENT_TABLE =
        "CREATE TABLE IF NOT EXISTS EVENT( " +
        " msg_offset bigint PRIMARY KEY NOT NULL," +
        " msg_key varchar NOT NULL," +
        " content_type varchar NULL," +
        " type varchar NOT NULL," +
        " created_utc timestamp NOT NULL," +
        " data text NULL," +
        " event_size int NOT NULL" +
        ");";

    static final String OFFSET_TABLE =
        "CREATE TABLE IF NOT EXISTS OFFSET( " +
        " id INTEGER PRIMARY KEY AUTOINCREMENT," +
        " name varchar UNIQUE NOT NULL," +
        " value bigint NOT NULL" +
        ");";

    static final String PIPE_STATE_TABLE =
        "CREATE TABLE IF NOT EXISTS PIPE_STATE( " +
        " name varchar UNIQUE NOT NULL," +
        " value varchar NOT NULL" +
        ");";

    static final String INSERT_EVENT =
        "INSERT INTO EVENT (msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES (?,?,?,?,?,?,?);";

    static final String UPSERT_OFFSET =
        "INSERT INTO OFFSET (name, value) VALUES (?,?)" +
        " ON CONFLICT(name) DO UPDATE SET VALUE = ?;";

    static final String COMPACT =
        "DELETE FROM EVENT WHERE created_utc <= ? AND msg_offset NOT IN (SELECT max(msg_offset) FROM EVENT WHERE created_utc <= ? GROUP BY msg_key, type);";

    static final String COMPACT_DELETIONS =
        "DELETE FROM EVENT WHERE created_utc <= ? AND data IS NULL;";

    static final String OFFSET_CONSISTENCY_SUM =
        "SELECT SUM(max_offset_by_key_type) FROM ( " +
            "SELECT MAX(msg_offset) AS max_offset_by_key_type FROM EVENT WHERE msg_key || type NOT IN ( " +
                "SELECT msg_key || type FROM event WHERE msg_offset IN ( " +
                    "SELECT MAX(msg_offset) FROM EVENT WHERE msg_offset <= ? GROUP BY msg_key, type " +
                ") AND data IS NULL " +
            ") AND msg_offset <= ? GROUP BY msg_key, type " +
        ");";

    static final String CHOOSE_MAX_OFFSET =
        "SELECT max(msg_offset) FROM EVENT WHERE created_utc <= ?;";

    static final String DELETE_EVENTS = "DELETE FROM EVENT;";
    static final String DELETE_OFFSETS = "DELETE FROM OFFSET";
    static final String DELETE_PIPE_STATE = "DELETE FROM PIPE_STATE";
    static final String VACUUM_DB = "VACUUM;";
    static final String CHECKPOINT_DB = "PRAGMA wal_checkpoint(TRUNCATE);";
    static final String QUICK_INTEGRITY_CHECK = "PRAGMA quick_check(1);";
    static final String FULL_INTEGRITY_CHECK = "PRAGMA integrity_check;";

    static String getReadEvent(final int typesCount, final long maxBatchSize) {
        final StringBuilder queryBuilder = new StringBuilder()
            .append(" SELECT type, msg_key, content_type, msg_offset, created_utc, data, event_size ")
            .append(" FROM ")
            .append("    ( ")
            .append("       SELECT ")
            .append("         \"type\", msg_key, content_type, msg_offset, created_utc, \"data\", event_size, ")
            .append("         SUM(event.event_size) OVER (ORDER BY event.msg_offset ASC) AS running_size ")
            .append("       FROM event ")
            .append("       WHERE msg_offset >= ? ");

        appendFilterByTypes(queryBuilder, typesCount);

        queryBuilder
            .append("       ORDER BY msg_offset ASC ")
            .append("       LIMIT ?")
            .append("    ) unused ")
            .append(" WHERE running_size <  ").append(maxBatchSize)
            .append(" ORDER BY msg_offset ASC;")
        ;

        return queryBuilder.toString();
    }

    static String getOffset(final OffsetName name) {
        return "SELECT name, value FROM OFFSET WHERE name = '" + name.toString() + "';";
    }

    static void appendFilterByTypes(final StringBuilder queryBuilder, int typesCount) {
        if (typesCount != 0) {
            queryBuilder
                .append(" AND type IN (")
                .append(generateQuestionMarks(typesCount))
                .append(")");
        }
    }

    static String maxOffsetForConsumersQuery(final int typesCount) {
        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder
            .append("SELECT max(msg_offset) FROM event ")
            .append(" WHERE type IN (")
            .append(generateQuestionMarks(typesCount))
            .append(")")
            .append(";");

        return queryBuilder.toString();
    }

    private static String generateQuestionMarks(final int types) {
        return Stream.generate(() -> "?").limit(types).collect(Collectors.joining(","));
    }
}
