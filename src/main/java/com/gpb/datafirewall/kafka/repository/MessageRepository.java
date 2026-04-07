package com.gpb.datafirewall.kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.gpb.datafirewall.kafka.model.MessageEntity;

public interface MessageRepository extends JpaRepository<MessageEntity, Long> {
}
