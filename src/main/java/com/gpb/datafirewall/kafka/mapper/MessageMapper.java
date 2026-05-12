package com.gpb.datafirewall.kafka.mapper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gpb.datafirewall.kafka.dto.MessageDto;
import com.gpb.datafirewall.kafka.model.MessageEntity;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class MessageMapper {

    @Value("${spring.datasource.username}")
    private String username;

    public MessageEntity toEntity(MessageDto dto) {
        MessageEntity entity = new MessageEntity();

        entity.setEventId(dto.eventId());
        entity.setUserLogin(username);
        entity.setActionType(dto.actionType());
        entity.setActionDttm(dto.actionDttm());
        entity.setKafkaTimestamp(dto.kafkaTimestamp());
        entity.setKafkaPartition(dto.kafkaPartition());
        entity.setKafkaOffset(dto.kafkaOffset());
        entity.setDataJson(dto.dataJson());

        return entity;
    }
}
