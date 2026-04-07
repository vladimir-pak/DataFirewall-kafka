package com.gpb.datafirewall.kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.gpb.datafirewall.kafka.model.TableName;

/**
 * JpaRepository для взаимодействия с базой
 */
public interface TableNameRepository extends JpaRepository<TableName, Long> {

}
