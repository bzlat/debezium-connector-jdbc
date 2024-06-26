package io.debezium.connector.jdbc.naming;

import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * Custom implementation of the {@link TableNamingStrategy} where the table name is derived
 * from the {@code __dbz__physicalTableIdentifier} record key.
 * The goal is to allow processing CDC events from a single topic and persisting them in
 * separate tables.
 *
 * @author Bogdan Zlatanov
 */
public class RecordKeyTableNamingStrategy implements TableNamingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordKeyTableNamingStrategy.class);

    public static final String DBZ_PHYSICAL_TABLE_IDENTIFIER = "__dbz__physicalTableIdentifier";

    @Override
    public String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record) {
        final Schema keySchema = record.keySchema();
        if (keySchema != null && Schema.Type.STRUCT.equals(keySchema.type())) {
            final Struct recordKey = (Struct) record.key();
            final String sourceTableName = recordKey.getString(DBZ_PHYSICAL_TABLE_IDENTIFIER);
            final String table = sourceTableName.replace(".", "_");
            return table;
        }
        throw new ConnectException("No struct-based primary key defined for record key.");
    }
}
