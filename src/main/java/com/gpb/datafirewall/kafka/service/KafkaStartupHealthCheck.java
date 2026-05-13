package com.gpb.datafirewall.kafka.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class KafkaStartupHealthCheck {

    private final KafkaProperties kafkaProperties;
    private final SslBundles sslBundles;

    @PostConstruct
    public void checkKafkaConnection() {
        try (AdminClient adminClient = AdminClient.create(kafkaProperties.buildAdminProperties(sslBundles))) {
            adminClient.describeCluster()
                    .nodes()
                    .get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka is not available. Application startup aborted.", e);
        }
    }
}