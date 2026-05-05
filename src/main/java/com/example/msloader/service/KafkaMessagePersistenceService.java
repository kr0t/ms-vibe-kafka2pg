package com.example.msloader.service;

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

    public KafkaMessagePersistenceService(
            FairTopicBuffer fairTopicBuffer,
            KafkaMessageRepository kafkaMessageRepository,
            LoaderProperties loaderProperties) {
        this.fairTopicBuffer = fairTopicBuffer;
        this.kafkaMessageRepository = kafkaMessageRepository;
        this.loaderProperties = loaderProperties;
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
        entity.setTopicName(message.topicName());
        entity.setPartitionId(message.partitionId());
        entity.setMessageOffset(message.messageOffset());
        entity.setMessageKey(message.messageKey());
        entity.setMessageValue(message.messageValue());
        entity.setHeadersJson(message.headersJson());
        entity.setKafkaTimestamp(message.kafkaTimestamp());
        entity.setSavedAt(savedAt);
        return entity;
    }
}
