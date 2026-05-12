package com.gpb.datafirewall.kafka.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record KafkaMessagePayload (
        String eventId,
        JsonNode requestJson,
        JsonNode shortAnswerJson,
        JsonNode detailAnswerJson,
        String actionDttm,
        String status
) {
}
