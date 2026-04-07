package com.gpb.datafirewall.kafka.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;

import com.gpb.datafirewall.kafka.cef.SvoiLogger;
import com.gpb.datafirewall.kafka.cef.enums.SvoiSeverityEnum;
import com.gpb.datafirewall.kafka.model.TableName;
import com.gpb.datafirewall.kafka.repository.TableNameRepository;
import com.gpb.datafirewall.kafka.service.CustomService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Реализация бизнес-логики
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CustomServiceImpl implements CustomService {
    private final TableNameRepository tableNameRepository;
    private final SvoiLogger logger;

    @Override
    public void start() {
        List<TableName> entities = tableNameRepository.findAll();
        if (!entities.isEmpty()) {
            log.info("There are some entities");
            /* Логируем CEF */
            logger.sendInternal(
                    "ExecuteStart", 
                    "MethodStart", 
                    "Start method executed",
                    SvoiSeverityEnum.ONE);
        }
    }
}
