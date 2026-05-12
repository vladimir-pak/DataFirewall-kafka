package com.gpb.datafirewall.kafka.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.gpb.datafirewall.kafka.dto.MessageDto;
import com.gpb.datafirewall.kafka.mapper.MessageMapper;
import com.gpb.datafirewall.kafka.model.MessageEntity;
import com.gpb.datafirewall.kafka.repository.MessageRepository;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageService {

    private final MessageRepository repository;
    private final MessageMapper mapper;

    @Transactional
    public void saveAll(List<MessageDto> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        List<MessageEntity> entities = mapper.toEntities(messages);
        repository.saveAll(entities);
    }

    @Transactional
    public void save(MessageDto message) {
        saveAll(List.of(message));
    }
}
