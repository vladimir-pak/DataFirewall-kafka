package com.gpb.datafirewall.kafka.model;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.databind.JsonNode;

public record MessageEntity(
        String eventId,
        String actionType,
        OffsetDateTime actionDttm,
        Long kafkaTimestamp,
        String kafkaPartition,
        Long kafkaOffset,
        JsonNode dataJson,
        OffsetDateTime createdAt
) {
}
