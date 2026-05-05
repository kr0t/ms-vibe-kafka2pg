package com.example.msloader.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.msloader.domain.BufferedKafkaMessage;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageListener {

    private final FairTopicBuffer fairTopicBuffer;
    private final KafkaHeaderSerializer kafkaHeaderSerializer;
    private final ObjectMapper objectMapper;

    public KafkaMessageListener(
            FairTopicBuffer fairTopicBuffer,
            KafkaHeaderSerializer kafkaHeaderSerializer,
            ObjectMapper objectMapper) {
        this.fairTopicBuffer = fairTopicBuffer;
        this.kafkaHeaderSerializer = kafkaHeaderSerializer;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "kafka-loader-listener",
            topics = "#{'${app.loader.topics}'.split(',')}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws InterruptedException {
        fairTopicBuffer.offer(new BufferedKafkaMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                parsePayload(record.value()),
                kafkaHeaderSerializer.toJson(record.headers()),
                Instant.ofEpochMilli(record.timestamp()),
                Instant.now()
        ));
        acknowledgment.acknowledge();
    }

    private com.fasterxml.jackson.databind.JsonNode parsePayload(String payload) {
        try {
            return objectMapper.readTree(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Message payload is not valid JSON", exception);
        }
    }
}
