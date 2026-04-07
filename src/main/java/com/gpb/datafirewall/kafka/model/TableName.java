package com.gpb.datafirewall.kafka.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;

/**
 * Пример сущности таблицы
 */
@Entity
@Data
@Table(name = "table_name", schema = "schema")
public class TableName {
    private Long id;
}
