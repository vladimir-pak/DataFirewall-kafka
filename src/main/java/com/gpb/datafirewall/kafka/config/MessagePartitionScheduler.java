package com.gpb.datafirewall.kafka.config;

import com.gpb.datafirewall.kafka.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import java.time.LocalDate;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessagePartitionScheduler {

    private final MessageRepository partitionRepository;

    @Value("${app.journal.create-days-ahead:3}")
    private int createDaysAhead;

    @Value("${app.journal.cleanup-period:360}")
    private int cleanupPeriod;

    @PostConstruct
    public void createPartitionsOnStartup() {
        maintainPartitions();
    }

    @Scheduled(cron = "${app.journal.partition-cron}")
    public void createPartitionsBySchedule() {
        maintainPartitions();
    }

    private void maintainPartitions() {
        createPartitions();
        cleanupOldPartitions();
    }

    private void createPartitions() {
        LocalDate today = LocalDate.now();

        for (int i = 0; i <= createDaysAhead; i++) {
            LocalDate day = today.plusDays(i);
            partitionRepository.createDailyPartitionIfNotExists(day);
            log.info("Создана партиция на={}", day);
        }
    }

    private void cleanupOldPartitions() {
        LocalDate today = LocalDate.now();
        LocalDate thresholdDate = today.minusDays(cleanupPeriod);

        int dropped = partitionRepository.dropPartitionsOlderThan(thresholdDate);

        log.info(
                "Удаление старых партиций завершено. cleanupPeriod={}, thresholdDate={}, dropped={}",
                cleanupPeriod,
                thresholdDate,
                dropped
        );
    }
}