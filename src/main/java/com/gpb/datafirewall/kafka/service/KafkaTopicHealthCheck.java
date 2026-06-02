package com.gpb.datafirewall.kafka.service;

import com.gpb.datafirewall.kafka.cef.SvoiLogger;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaTopicHealthCheck {

    private final KafkaProperties kafkaProperties;
    private final SslBundles sslBundles;
    private final SvoiLogger svoiLogger;

    @Value("${app.kafka.topic:datafirewall.logs}")
    private String mainTopic;

    @Value("${app.kafka.audit-topic:datafirewall.audit}")
    private String auditTopic;

    @Value("${app.kafka.topic-healthcheck.enabled:true}")
    private boolean enabled;

    @PostConstruct
    public void checkTopics() {
        if (!enabled) {
            log.info("Kafka topic healthcheck disabled");
            return;
        }

        List<String> topics = List.of(mainTopic, auditTopic);

        try (AdminClient adminClient = AdminClient.create(kafkaProperties.buildAdminProperties(sslBundles))) {
            Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(topics)
                    .allTopicNames()
                    .get(15, TimeUnit.SECONDS);

            descriptions.forEach((topicName, description) -> {
                int partitionsCount = description.partitions().size();

                log.info(
                        "Kafka topic connection success. topic={}, partitionsCount={}",
                        topicName,
                        partitionsCount
                );

                sendSuccessCef(topicName, partitionsCount);
            });

        } catch (Exception e) {
            log.error("Kafka topic healthcheck failed. topics={}", topics, e);

            sendErrorCef(topics, e);

            throw new IllegalStateException("Kafka topics are not available. Application startup aborted.", e);
        }
    }

    private void sendSuccessCef(String topicName, int partitionsCount) {
        svoiLogger.sendKafkaMessage(
                "Успешное подключение к Kafka topic. topic=" + topicName
                        + ", partitionsCount=" + partitionsCount,
                resolveKafkaUser(),
                resolveBootstrapServers(),
                topicName,
                resolveKafkaPort()
        );
    }

    private void sendErrorCef(List<String> topics, Exception e) {
        svoiLogger.sendKafkaMessage(
                "Ошибка подключения к Kafka topic. topics=" + topics
                        + ", error=" + e.getMessage(),
                resolveKafkaUser(),
                resolveBootstrapServers(),
                String.join(",", topics),
                resolveKafkaPort()
        );
    }

    private String resolveBootstrapServers() {
        List<String> servers = kafkaProperties.getBootstrapServers();

        if (servers == null || servers.isEmpty()) {
            return "UNDEFINED";
        }

        return String.join(",", servers);
    }

    private Integer resolveKafkaPort() {
        List<String> servers = kafkaProperties.getBootstrapServers();

        if (servers == null || servers.isEmpty()) {
            return -1;
        }

        String first = servers.get(0);
        int colonIndex = first.lastIndexOf(':');

        if (colonIndex < 0 || colonIndex == first.length() - 1) {
            return -1;
        }

        return Integer.parseInt(first.substring(colonIndex + 1));
    }

    private String resolveKafkaUser() {
        Map<String, Object> props = kafkaProperties.buildAdminProperties(sslBundles);

        Object jaasConfig = props.get("sasl.jaas.config");

        if (jaasConfig == null) {
            return "UNDEFINED";
        }

        String value = String.valueOf(jaasConfig);

        String marker = "username=\"";
        int start = value.indexOf(marker);

        if (start < 0) {
            return "UNDEFINED";
        }

        int usernameStart = start + marker.length();
        int usernameEnd = value.indexOf("\"", usernameStart);

        if (usernameEnd < 0) {
            return "UNDEFINED";
        }

        return value.substring(usernameStart, usernameEnd);
    }
}