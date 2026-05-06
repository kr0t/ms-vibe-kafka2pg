package com.example.msloader.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.msloader.config.LoaderProperties;
import com.example.msloader.domain.BufferedKafkaMessage;
import com.example.msloader.domain.KafkaMessageEntity;
import com.example.msloader.repository.KafkaMessageRepository;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessagePersistenceService {

    private final FairTopicBuffer fairTopicBuffer;
    private final KafkaMessageRepository kafkaMessageRepository;
    private final LoaderProperties loaderProperties;
    private final ObjectMapper objectMapper;

    public KafkaMessagePersistenceService(
            FairTopicBuffer fairTopicBuffer,
            KafkaMessageRepository kafkaMessageRepository,
            LoaderProperties loaderProperties,
            ObjectMapper objectMapper) {
        this.fairTopicBuffer = fairTopicBuffer;
        this.kafkaMessageRepository = kafkaMessageRepository;
        this.loaderProperties = loaderProperties;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${app.loader.writer-delay-ms}")
    @Transactional
    public void persistBufferedMessages() {
        List<BufferedKafkaMessage> batch = fairTopicBuffer.drainFairBatch(loaderProperties.getWriterBatchSize());
        if (batch.isEmpty()) {
            return;
        }

        Instant savedAt = Instant.now();
        List<KafkaMessageEntity> entities = batch.stream()
                .sorted(Comparator.comparing(BufferedKafkaMessage::kafkaTimestamp)
                        .thenComparing(BufferedKafkaMessage::bufferedAt))
                .map(message -> toEntity(message, savedAt))
                .toList();

        kafkaMessageRepository.saveAll(entities);
    }

    private KafkaMessageEntity toEntity(BufferedKafkaMessage message, Instant savedAt) {
        KafkaMessageEntity entity = new KafkaMessageEntity();
        entity.setMsgKey(message.messageKey() == null ? "" : message.messageKey());
        entity.setValueJson(writeJson(message.messageValue()));
        entity.setHeadersIson(writeJson(message.headersJson()));
        entity.setTopic(message.topicName());
        entity.setQueryType(loaderProperties.getQueryType());
        entity.setStatus(loaderProperties.getStatus());
        entity.setKafkaDttm(message.kafkaTimestamp());
        entity.setStartDttm(message.bufferedAt());
        entity.setCompleteDttm(savedAt);
        entity.setLockedAt(null);
        entity.setNamespace(loaderProperties.getNamespace());
        entity.setPodName(loaderProperties.getPodName());
        entity.setDescription(loaderProperties.getDescription());
        return entity;
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize message payload for database", exception);
        }
    }
}
