package com.gpb.datafirewall.kafka.dto;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.databind.JsonNode;

public record MessageDto(
        String userLogin,
        String actionType,
        OffsetDateTime actionDttm,
        Long kafkaTimestamp,
        String kafkaPartition,
        Long kafkaOffset,
        JsonNode dataJson
) {
}
