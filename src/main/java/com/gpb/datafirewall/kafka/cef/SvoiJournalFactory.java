package com.gpb.datafirewall.kafka.cef;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import com.gpb.datafirewall.kafka.cef.model.SvoiJournal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SvoiJournalFactory {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    private String deviceProduct;
    private String deviceVersion;
    
    @Value("${server.port:9200}")
    private Integer localPort;
    private String localHostName;
    private Long journalLineNumber = 0L;

    @Autowired(required = false)
    private BuildProperties buildProperties;

    public SvoiJournal getJournalSource() {
        SvoiJournal svoiJournal = this.getBaseJournal();
        HostInfo host = getLocalHostInfo();
        svoiJournal.setDst(host.ip);
        svoiJournal.setDvchost(host.name);
        svoiJournal.setDpt(localPort);
        /* По умолчанию адрес источника запроса = адресу приложения */
        svoiJournal.setSrc(host.ip);
        svoiJournal.setShost(host.name);
        svoiJournal.setSpt(localPort);
        svoiJournal.setStart(LocalDateTime.now());
        return svoiJournal;
    }

    private SvoiJournal getBaseJournal() {
        LocalDateTime now = LocalDateTime.now();
        SvoiJournal svoiJournal = new SvoiJournal(this.nextLineNumber(), DATE_FORMATTER);
        svoiJournal.setDeviceProduct(this.buildProperties != null ? this.buildProperties.getName() : this.deviceProduct);
        svoiJournal.setDeviceVersion(this.buildProperties != null ? this.buildProperties.getVersion() : this.deviceVersion);
        svoiJournal.setTime(now);
        svoiJournal.setApp("TCP");
        svoiJournal.setRt(now);
        svoiJournal.setDeviceProcessName("java");
        return svoiJournal;
    }

    private record HostInfo(String name, String ip) {
    }

    private HostInfo getLocalHostInfo() {
        try {
            InetAddress inet = InetAddress.getLocalHost();
            return new HostInfo(inet.getHostName(), inet.getHostAddress());
        } catch (Exception e) {
            InetAddress inet = InetAddress.getLoopbackAddress();
            return new HostInfo(inet.getHostName(), inet.getHostAddress());
        }
    }

    private synchronized Long nextLineNumber() {
        return this.journalLineNumber = this.journalLineNumber + 1L;
    }

    @EventListener({ApplicationStartedEvent.class})
    @Order(Integer.MIN_VALUE)
    public void init() {
        try {
            this.localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.localHostName = InetAddress.getLoopbackAddress().getHostName();
        }
    }
}

