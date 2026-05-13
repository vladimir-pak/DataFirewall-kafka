package com.gpb.datafirewall.kafka.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
public final class JsonDateTimeNormalizer {

    private static final ZoneId TARGET_ZONE = ZoneId.of("UTC");

    private static final DateTimeFormatter TARGET_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(TARGET_ZONE);

    private JsonDateTimeNormalizer() {
    }

    public static JsonNode normalizeDfwDateFields(JsonNode source) {
        if (source == null || source.isNull()) {
            return source;
        }

        if (!source.isObject()) {
            return source;
        }

        ObjectNode objectNode = source.deepCopy();

        normalizeIsoDateTime(objectNode, "dfw_action_dttm");
        normalizeUnixDateTime(objectNode, "dfw_created_dttm");
        normalizeUnixDateTime(objectNode, "dfw_readed_dttm");

        return objectNode;
    }

    private static void normalizeIsoDateTime(ObjectNode node, String fieldName) {
        JsonNode value = node.get(fieldName);

        if (value == null || value.isNull()) {
            return;
        }

        String rawValue = value.asText(null);

        if (rawValue == null || rawValue.isBlank()) {
            return;
        }

        try {
            Instant instant = OffsetDateTime.parse(rawValue).toInstant();
            node.put(fieldName, TARGET_FORMATTER.format(instant));
        } catch (Exception e) {
            log.warn("Cannot parse ISO datetime field={}, value={}", fieldName, rawValue, e);
        }
    }

    private static void normalizeUnixDateTime(ObjectNode node, String fieldName) {
        JsonNode value = node.get(fieldName);

        if (value == null || value.isNull()) {
            return;
        }

        try {
            long unixValue;

            if (value.isNumber()) {
                unixValue = value.asLong();
            } else {
                String rawValue = value.asText(null);

                if (rawValue == null || rawValue.isBlank()) {
                    return;
                }

                unixValue = Long.parseLong(rawValue);
            }

            Instant instant = toInstantFromUnix(unixValue);
            node.put(fieldName, TARGET_FORMATTER.format(instant));
        } catch (Exception e) {
            log.warn("Cannot parse Unix datetime field={}, value={}", fieldName, value, e);
        }
    }

    private static Instant toInstantFromUnix(long unixValue) {
        /*
         * Поддержка двух популярных вариантов:
         * 1715529976    — Unix seconds
         * 1715529976123 — Unix milliseconds
         */
        if (Math.abs(unixValue) < 10_000_000_000L) {
            return Instant.ofEpochSecond(unixValue);
        }

        return Instant.ofEpochMilli(unixValue);
    }
}
