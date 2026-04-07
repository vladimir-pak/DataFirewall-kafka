package com.gpb.datafirewall.kafka.cef;

import java.net.NetworkInterface;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import com.gpb.datafirewall.kafka.cef.enums.SvoiSeverityEnum;
import com.gpb.datafirewall.kafka.cef.model.Log;
import com.gpb.datafirewall.kafka.cef.model.SvoiJournal;
import com.gpb.datafirewall.kafka.cef.properties.LogsDatabaseProperties;
import com.gpb.datafirewall.kafka.cef.properties.SysProperties;
import com.gpb.datafirewall.kafka.cef.repository.LogRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Класс - логгер по стандартам СВОИ (CEF)
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SvoiLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final SvoiJournalFactory svoiJournalFactory = new SvoiJournalFactory();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

    public void sendInternal(String deviceEventClassID,
                             String name,
                             String message,
                             SvoiSeverityEnum severity) {

        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        send(deviceEventClassID, name, message, severity, journal);
    }

    private void send(String deviceEventClassID,
                      String name,
                      String message,
                      SvoiSeverityEnum severity,
                      SvoiJournal journal) {

        defaultArgs(journal, deviceEventClassID, name, message, severity);

        try (MDC.MDCCloseable a = MDC.putCloseable("host", journal.getHostForSvoi());
             MDC.MDCCloseable b = MDC.putCloseable("log_type", "audit_log")) {

            log.info(journalToString(journal));

        }
        if (!logsDatabaseProperties.isEnabled()) return;

        try {
            LocalDateTime created = LocalDateTime.parse(journal.getStart(), FORMATTER);

            logRepository.save(new Log(
                    created,
                    journalToString(journal),
                    deviceEventClassID
            ));
        } catch (Exception e) {
            log.error("Ошибка при сохранении лога в БД", e);
        }
    }

    private void defaultArgs(SvoiJournal journal,
            String deviceEventClassID,
            String name,
            String message,
            SvoiSeverityEnum severity) {

        journal.setDeviceProduct(sysProperties.getName());
        journal.setDeviceVersion(sysProperties.getVersion());
        journal.setDntdom(sysProperties.getDntdom());
        journal.setDeviceEventClassID(deviceEventClassID);
        journal.setName(name);
        journal.setMessage(message);
        if (journal.getDuser() == null) {
            journal.setDuser(sysProperties.getUser());
        }
        if (journal.getSuser() == null) {
            journal.setSuser(sysProperties.getUser());
        }
        if (journal.getApp() == null) {
            journal.setApp("TCP");
        }
        journal.setDmac(getMacAddress());
        journal.setSeverity(severity);
        if (journal.getSpt() == null) {
            journal.setSpt(sysProperties.getDpt());
        }
        if (journal.getDpt() == null) {
            journal.setDpt(sysProperties.getDpt());
        }
    }

    private String journalToString(SvoiJournal journal) {
        return journal.toString();
    }

    private String getMacAddress() {
        List<String> macs = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                byte[] mac = interfaces.nextElement().getHardwareAddress();
                if (mac == null) continue;
                for (byte b : mac) macs.add(String.format("%02X", b));
            }
        } catch (Exception ignored) {
        }
        return String.join(":", macs);
    }
}
