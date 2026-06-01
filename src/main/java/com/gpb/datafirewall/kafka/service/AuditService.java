package com.gpb.datafirewall.kafka.service;

import java.time.OffsetDateTime;

import org.springframework.stereotype.Service;

import com.gpb.datafirewall.kafka.repository.MessageRepository;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditService {

    private final MessageRepository repository;

    @Transactional
    public void save(String log, OffsetDateTime created) {
        if (log == null || log.isEmpty()) {
            return;
        }

        repository.saveAudit(log, created);
    }
}
