package com.gpb.datafirewall.kafka.mapper;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.kafka.dto.MessageDto;
import com.gpb.datafirewall.kafka.model.MessageEntity;

@Component
public class MessageMapper {

    private final ObjectMapper objectMapper;

    public MessageMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public MessageEntity toEntity(MessageDto dto) {
        MessageEntity entity = new MessageEntity();
        entity.setUserLogin(dto.userLogin());
        entity.setActionType(dto.actionType());
        entity.setActionDttm(dto.actionDttm());
        entity.setKafkaTimestamp(dto.kafkaTimestamp());
        entity.setKafkaPartition(dto.kafkaPartition());
        entity.setKafkaOffset(dto.kafkaOffset());

        try {
            entity.setDataJson(dto.dataJson() == null ? null : objectMapper.writeValueAsString(dto.dataJson()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize dataJson", e);
        }

        return entity;
    }
}
