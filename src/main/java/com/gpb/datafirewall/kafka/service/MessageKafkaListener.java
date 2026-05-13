package com.gpb.datafirewall.kafka.service;

import java.time.OffsetDateTime;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.kafka.dto.KafkaMessagePayload;
import com.gpb.datafirewall.kafka.dto.MessageDto;

import lombok.RequiredArgsConstructor;

import static com.gpb.datafirewall.kafka.utils.JsonDateTimeNormalizer.normalizeDfwDateFields;

@Component
@RequiredArgsConstructor
public class MessageKafkaListener {

    private final ObjectMapper objectMapper;
    private final MessageService messageService;

    @KafkaListener(
            topics = "${app.kafka.topic:user-actions}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws Exception {
        KafkaMessagePayload payload = objectMapper.readValue(record.value(), KafkaMessagePayload.class);

        JsonNode normalizedShortAnswerJson = normalizeDfwDateFields(payload.shortAnswerJson());
        JsonNode normalizedDetailAnswerJson = normalizeDfwDateFields(payload.detailAnswerJson());

        MessageDto query = new MessageDto(
                payload.eventId(),
                "QUERY",
                OffsetDateTime.parse(payload.actionDttm()),
                record.timestamp(),
                String.valueOf(record.partition()),
                record.offset(),
                payload.requestJson()
        );

        MessageDto answer = new MessageDto(
                payload.eventId(),
                "ANSWER",
                OffsetDateTime.parse(payload.actionDttm()),
                record.timestamp(),
                String.valueOf(record.partition()),
                record.offset(),
                normalizedShortAnswerJson
        );

        MessageDto answerDetail = new MessageDto(
                payload.eventId(),
                "ANSWER_DETAIL",
                OffsetDateTime.parse(payload.actionDttm()),
                record.timestamp(),
                String.valueOf(record.partition()),
                record.offset(),
                normalizedDetailAnswerJson
        );

        List<MessageDto> rows = List.of(query, answer, answerDetail);

        messageService.saveAll(rows);
        acknowledgment.acknowledge();
    }
}
