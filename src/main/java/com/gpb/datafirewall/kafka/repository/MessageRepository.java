package com.gpb.datafirewall.kafka.repository;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.kafka.model.MessageEntity;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class MessageRepository {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public void saveAll(List<MessageEntity> entities) {
        String sql = """
            insert into df_meta.datafirewall_journal (
                event_id,
                action_type,
                action_dttm,
                kafka_timestamp,
                kafka_partition,
                kafka_offset,
                data_json,
                created_at
            )
            values (?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            on conflict (
                kafka_partition,
                kafka_offset,
                action_type,
                action_dttm
            ) do nothing
            """;

        jdbcTemplate.batchUpdate(sql, entities, 500, (ps, entity) -> {
            ps.setString(1, entity.eventId());
            ps.setString(2, entity.actionType());
            ps.setObject(3, entity.actionDttm());
            ps.setLong(4, entity.kafkaTimestamp());
            ps.setString(5, entity.kafkaPartition());
            ps.setLong(6, entity.kafkaOffset());

            try {
                ps.setString(
                        7,
                        entity.dataJson() == null
                                ? null
                                : objectMapper.writeValueAsString(entity.dataJson())
                );
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Cannot serialize dataJson", e);
            }

            ps.setObject(8, entity.createdAt());
        });
    }
}
