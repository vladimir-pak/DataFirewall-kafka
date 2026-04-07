package com.gpb.datafirewall.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.kafka.dto.KafkaMessagePayload;
import com.gpb.datafirewall.kafka.dto.MessageDto;

import lombok.RequiredArgsConstructor;

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

        MessageDto dto = new MessageDto(
                payload.userLogin(),
                payload.actionType(),
                payload.actionDttm(),
                record.timestamp(),
                String.valueOf(record.partition()),
                record.offset(),
                payload.dataJson()
        );

        messageService.save(dto);
        acknowledgment.acknowledge();
    }
}
