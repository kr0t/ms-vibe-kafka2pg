package com.example.msloader.repository;

import com.example.msloader.domain.KafkaMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KafkaMessageRepository extends JpaRepository<KafkaMessageEntity, Long> {
}
