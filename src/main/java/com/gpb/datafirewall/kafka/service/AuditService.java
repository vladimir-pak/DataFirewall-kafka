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

        String type = resolveType(log);

        repository.saveAudit(log, created, type);
    }

    private String resolveType(String log) {
        int p1 = log.indexOf('|');
        if (p1 < 0) return null;

        int p2 = log.indexOf('|', p1 + 1);
        if (p2 < 0) return null;

        int p3 = log.indexOf('|', p2 + 1);
        if (p3 < 0) return null;

        int p4 = log.indexOf('|', p3 + 1);
        if (p4 < 0) return null;

        int p5 = log.indexOf('|', p4 + 1);
        if (p5 < 0) return null;

        return log.substring(p4 + 1, p5);
    }
}
