package com.gpb.datafirewall.kafka.dto;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.databind.JsonNode;

public record KafkaMessagePayload (
        String userLogin,
        String actionType,
        OffsetDateTime actionDttm,
        JsonNode dataJson
) {
}
