package com.gpb.datafirewall.kafka.model;

import java.time.OffsetDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(
        name = "user_actions",
        uniqueConstraints = {
                @UniqueConstraint(name = "ux_user_actions_partition_offset",
                        columnNames = {"kafka_partition", "kafka_offset"})
        }
)
public class MessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_login", nullable = false)
    private String userLogin;

    @Column(name = "action_type", nullable = false)
    private String actionType;

    @Column(name = "action_dttm", nullable = false)
    private OffsetDateTime actionDttm;

    @Column(name = "kafka_timestamp", nullable = false)
    private Long kafkaTimestamp;

    @Column(name = "kafka_partition", nullable = false)
    private String kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private Long kafkaOffset;

    @Column(name = "data_json", columnDefinition = "jsonb")
    private String dataJson;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    public MessageEntity() {
    }

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = OffsetDateTime.now();
        }
    }

    public Long getId() {
        return id;
    }

    public String getUserLogin() {
        return userLogin;
    }

    public void setUserLogin(String userLogin) {
        this.userLogin = userLogin;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public OffsetDateTime getActionDttm() {
        return actionDttm;
    }

    public void setActionDttm(OffsetDateTime actionDttm) {
        this.actionDttm = actionDttm;
    }

    public Long getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public void setKafkaTimestamp(Long kafkaTimestamp) {
        this.kafkaTimestamp = kafkaTimestamp;
    }

    public String getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(String kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public Long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(Long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public String getDataJson() {
        return dataJson;
    }

    public void setDataJson(String dataJson) {
        this.dataJson = dataJson;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
