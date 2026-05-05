package com.example.msloader.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public record BufferedKafkaMessage(
        String topicName,
        int partitionId,
        long messageOffset,
        String messageKey,
        JsonNode messageValue,
        JsonNode headersJson,
        Instant kafkaTimestamp,
        Instant bufferedAt
) {
}
