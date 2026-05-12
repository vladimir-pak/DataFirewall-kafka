package com.gpb.datafirewall.kafka.repository;

import java.time.LocalDate;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
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

    @Value("${app.journal.schema:df_meta}")
    private String schema;

    @Value("${app.journal.table:datafirewall_journal}")
    private String table;

    public void saveAll(List<MessageEntity> entities) {
        String sql = String.format("""
            insert into %s.%s (
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
        """, schema, table);

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

    public void createDailyPartitionIfNotExists(LocalDate day) {
        validateIdentifier(schema);
        validateIdentifier(table);

        String partitionName = table + "_" + day.toString().replace("-", "_");
        validateIdentifier(partitionName);

        LocalDate from = day;
        LocalDate to = day.plusDays(1);

        String sql = """
            create table if not exists %s.%s
            partition of %s.%s
            for values from ('%s 00:00:00+00') to ('%s 00:00:00+00')
            """.formatted(
                schema,
                partitionName,
                schema,
                table,
                from,
                to
        );

        jdbcTemplate.execute(sql);
    }

    public int dropPartitionsOlderThan(LocalDate thresholdDate) {
        validateIdentifier(schema);
        validateIdentifier(table);

        String prefix = table + "_";

        String sql = """
            select tablename
            from pg_tables
            where schemaname = ?
            and tablename like ?
            """;

        List<String> partitionNames = jdbcTemplate.queryForList(
                sql,
                String.class,
                schema,
                prefix + "%"
        );

        int dropped = 0;

        for (String partitionName : partitionNames) {
            LocalDate partitionDate = extractDateFromPartitionName(partitionName);

            if (partitionDate == null) {
                continue;
            }

            if (partitionDate.isBefore(thresholdDate)) {
                validateIdentifier(partitionName);

                String dropSql = """
                    drop table if exists %s.%s
                    """.formatted(schema, partitionName);

                jdbcTemplate.execute(dropSql);
                dropped++;
            }
        }

        return dropped;
    }

    private LocalDate extractDateFromPartitionName(String partitionName) {
        String prefix = table + "_";

        if (!partitionName.startsWith(prefix)) {
            return null;
        }

        String datePart = partitionName.substring(prefix.length());

        // ожидаем формат yyyy_MM_dd
        if (!datePart.matches("\\d{4}_\\d{2}_\\d{2}")) {
            return null;
        }

        return LocalDate.parse(datePart.replace("_", "-"));
    }

    private void validateIdentifier(String value) {
        if (value == null || !value.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + value);
        }
    }
}
