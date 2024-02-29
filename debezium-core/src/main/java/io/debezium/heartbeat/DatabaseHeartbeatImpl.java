/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.SchemaNameAdjuster;

/**
 *  Implementation of the heartbeat feature that allows for a DB query to be executed with every heartbeat.
 */
public class DatabaseHeartbeatImpl extends HeartbeatImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHeartbeatImpl.class);

    public static final String HEARTBEAT_ACTION_QUERY_PROPERTY_NAME = "heartbeat.action.query";

    public static final String HEARTBEAT_CREATE_DATEBASE = "CREATE DATABASE IF NOT EXISTS debezium";

    public static final String HEARTBEAT_CHECK_TABLE = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'debezium' AND TABLE_NAME = 'heartbeat'";

    public static final String HEARTBEAT_CREATE_TABLE = "CREATE TABLE debezium.heartbeat (id INT PRIMARY KEY,time TIMESTAMP)";

    public static final Field HEARTBEAT_ACTION_QUERY = Field.create(HEARTBEAT_ACTION_QUERY_PROPERTY_NAME)
            .withDisplayName("An optional query to execute with every heartbeat")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED_HEARTBEAT, 2))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The query executed with every heartbeat.");

    private final String heartBeatActionQuery;
    private final JdbcConnection jdbcConnection;
    private final HeartbeatErrorHandler errorHandler;

    public DatabaseHeartbeatImpl(Duration heartbeatInterval, String topicName, String key, JdbcConnection jdbcConnection, String heartBeatActionQuery,
                                 HeartbeatErrorHandler errorHandler, SchemaNameAdjuster schemaNameAdjuster) {
        super(heartbeatInterval, topicName, key, schemaNameAdjuster);

        this.heartBeatActionQuery = heartBeatActionQuery;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        try {
            // 查看库是否创建，表是否创建
            if (StringUtils.isEmpty(heartBeatActionQuery)) {
                jdbcConnection.execute(HEARTBEAT_CREATE_DATEBASE);
                // 查看表是否存在
                String tableName = "";
                try {
                    tableName = JdbcConnection.querySingleValue(jdbcConnection.connection(),
                            HEARTBEAT_CHECK_TABLE,
                            st -> {
                            }, rs -> rs == null ? "" : rs.getObject(1, String.class));
                }
                catch (Exception e) {
                    LOGGER.error("未查询到表：{}", e.getMessage());
                }
                if (StringUtils.isEmpty(tableName)) {
                    jdbcConnection.execute(HEARTBEAT_CREATE_TABLE);
                }
                jdbcConnection.execute("INSERT INTO debezium.heartbeat (id, time)VALUES (1, NOW()) ON DUPLICATE KEY UPDATE time = NOW()");
            }
            else {
                jdbcConnection.execute(heartBeatActionQuery);
            }
        }
        catch (SQLException e) {
            if (errorHandler != null) {
                errorHandler.onError(e);
            }
            LOGGER.error("Could not execute heartbeat action (Error: " + e.getSQLState() + ")", e);
        }
        LOGGER.debug("Executed heartbeat action query");

        super.forcedBeat(partition, offset, consumer);
    }

    @Override
    public void close() {
        try {
            jdbcConnection.close();
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing the heartbeat JDBC connection", e);
        }
    }
}
