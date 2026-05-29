package com.gpb.datafirewall.kafka.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import com.gpb.datafirewall.kafka.cef.SvoiLogger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class KafkaStartupHealthCheck {

    private final SvoiLogger svoiCustomLogger;
    private final KafkaProperties kafkaProperties;
    private final SslBundles sslBundles;

    @PostConstruct
    public void checkKafkaConnection() {
        try (AdminClient adminClient = AdminClient.create(kafkaProperties.buildAdminProperties(sslBundles))) {
            adminClient.describeCluster()
                    .nodes()
                    .get(15, TimeUnit.SECONDS);

            Map<String, Object> props = kafkaProperties.buildProducerProperties();
            String bootstrapServers = resolveBootstrapServers();
            String securityProtocol = stringValue(props.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            String mechanism = stringValue(props.get(SaslConfigs.SASL_MECHANISM));
            String username = resolveUsername(props);
            int port = resolvePort();

            String messageLog = String.format("securityProtocol: %s, mechanism: %s", securityProtocol, mechanism);

            svoiCustomLogger.sendKafkaMessage(
                messageLog,
                username,
                bootstrapServers,
                bootstrapServers,
                port
            );
            
        } catch (Exception e) {
            throw new IllegalStateException("Kafka is not available. Application startup aborted.", e);
        }
    }

    private String resolveBootstrapServers() {
        List<String> servers = kafkaProperties.getBootstrapServers();

        return servers.stream()
                .map(this::extractHost)
                .collect(Collectors.joining(", "));
    }

    private String extractHost(String bootstrapServer) {
        int colonIndex = bootstrapServer.indexOf(':');

        if (colonIndex == -1) {
            return bootstrapServer;
        }

        return bootstrapServer.substring(0, colonIndex);
    }

    private int resolvePort() {
        List<String> servers = kafkaProperties.getBootstrapServers();
        String hostname = servers.get(0);
        int colonIndex = hostname.indexOf(':');

        if (colonIndex == -1) {
            return colonIndex;
        }

        return Integer.parseInt(hostname.substring(1, colonIndex));
    }

    private String resolveUsername(Map<String, Object> props) {
        /*
         * SASL/PLAIN, SCRAM обычно содержит username в sasl.jaas.config.
         */
        String jaasConfig = stringValue(props.get(SaslConfigs.SASL_JAAS_CONFIG));

        if (jaasConfig != null) {
            String username = extractJaasValue(jaasConfig, "username");
            if (username != null) {
                return username;
            }

            String principal = extractJaasValue(jaasConfig, "principal");
            if (principal != null) {
                return principal;
            }
        }

        return "UNDEFINED";
    }

    private String extractJaasValue(String jaasConfig, String key) {
        Pattern pattern = Pattern.compile(key + "\\s*=\\s*\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(jaasConfig);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}