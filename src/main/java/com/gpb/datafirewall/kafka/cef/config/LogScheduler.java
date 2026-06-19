package com.gpb.datafirewall.kafka.cef.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gpb.datafirewall.kafka.cef.SvoiLogger;
import com.gpb.datafirewall.kafka.cef.enums.SvoiSeverityEnum;
import com.gpb.datafirewall.kafka.cef.properties.LogsDatabaseProperties;
import com.gpb.datafirewall.kafka.cef.repository.LogPartitionRepository;
import com.gpb.datafirewall.kafka.cef.service.LogFileService;

/**
 * Планировщик для чистки логов и создания партиций в базе
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LogScheduler {
    private final SvoiLogger svoiCustomLogger;
    private final LogPartitionRepository logPartitionRepository;
    private final LogFileService cefLogger;
    private final LogsDatabaseProperties logsDatabaseProperties;

    @Value("${app.journal.audit-table:dq_audit.datafirewall_log}")
    private String auditTable;

    @Scheduled(cron = "${logs-database.task-create-partition}")
    public void createPartition() {
        String tableName = logsDatabaseProperties.getTable().trim();
        logPartitionRepository.createTodayPartition(tableName);
        logPartitionRepository.createTodayPartition(auditTable);
    }

    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanPartition() {
        String tableName = logsDatabaseProperties.getTable().trim();
        logPartitionRepository.dropOldPartitions(tableName);
        logPartitionRepository.createTodayPartition(auditTable);
    }

    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanupOldLogs() {
        cefLogger.rotateLogFile();
        cefLogger.cleanupOldLogs();
        svoiCustomLogger.sendInternal(
                "cleanLogs",
                "Clean Log Files",
                "Cleaned old log files",
                SvoiSeverityEnum.ONE
        );
    }
}
