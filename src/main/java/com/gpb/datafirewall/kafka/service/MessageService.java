package com.gpb.datafirewall.kafka.service;

import java.util.List;

import org.springframework.dao.DataIntegrityViolationException;
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
    public void save(MessageDto dto) {
        MessageEntity entity = mapper.toEntity(dto);

        try {
            repository.save(entity);
        } catch (DataIntegrityViolationException e) {
            // Сообщение уже было обработано ранее — считаем операцию идемпотентной.
            // При желании можно дополнительно проверить constraint name.
        }
    }

    @Transactional
    public void saveAll(List<MessageDto> messages) {
        List<MessageEntity> entities = messages.stream()
                .map(mapper::toEntity)
                .toList();

        repository.saveAllAndFlush(entities);
    }
}
