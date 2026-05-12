package com.gpb.datafirewall.kafka.mapper;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.stereotype.Component;

import com.gpb.datafirewall.kafka.dto.MessageDto;
import com.gpb.datafirewall.kafka.model.MessageEntity;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class MessageMapper {

    public MessageEntity toEntity(MessageDto dto) {
        return new MessageEntity(
                dto.eventId(),
                dto.actionType(),
                dto.actionDttm(),
                dto.kafkaTimestamp(),
                dto.kafkaPartition(),
                dto.kafkaOffset(),
                dto.dataJson(),
                OffsetDateTime.now()
        );
    }

    public List<MessageEntity> toEntities(List<MessageDto> messages) {
        return messages.stream()
                .map(this::toEntity)
                .toList();
    }
}
