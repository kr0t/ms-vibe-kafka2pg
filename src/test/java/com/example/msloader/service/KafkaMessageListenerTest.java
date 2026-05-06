package com.example.msloader.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.msloader.domain.BufferedKafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

class KafkaMessageListenerTest {

    private final FairTopicBuffer fairTopicBuffer = mock(FairTopicBuffer.class);
    private final KafkaHeaderSerializer kafkaHeaderSerializer = mock(KafkaHeaderSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaMessageListener listener =
            new KafkaMessageListener(fairTopicBuffer, kafkaHeaderSerializer, objectMapper);

    @Test
    void shouldBufferParsedMessageAndAcknowledgeRecord() throws Exception {
        RecordHeaders headers = new RecordHeaders();
        headers.add("source", "listener-test".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("topic-1", 2, 15L, 1715000000000L, null, 0L, 0, 0, "test-key", "{\"id\":1}", headers);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(kafkaHeaderSerializer.toJson(headers)).thenReturn(objectMapper.createArrayNode().add("serialized"));

        listener.onMessage(record, acknowledgment);

        ArgumentCaptor<BufferedKafkaMessage> captor = ArgumentCaptor.forClass(BufferedKafkaMessage.class);
        verify(fairTopicBuffer).offer(captor.capture());
        BufferedKafkaMessage buffered = captor.getValue();
        assertThat(buffered.topicName()).isEqualTo("topic-1");
        assertThat(buffered.partitionId()).isEqualTo(2);
        assertThat(buffered.messageOffset()).isEqualTo(15L);
        assertThat(buffered.messageKey()).isEqualTo("test-key");
        assertThat(buffered.messageValue().get("id").asInt()).isEqualTo(1);
        assertThat(buffered.headersJson()).hasSize(1);
        assertThat(buffered.kafkaTimestamp()).isEqualTo(Instant.ofEpochMilli(1715000000000L));
        assertThat(buffered.bufferedAt()).isNotNull();
        verify(acknowledgment).acknowledge();
    }

    @Test
    void shouldRejectInvalidJsonPayload() throws Exception {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("topic-1", 0, 1L, "key", "not-json");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        assertThatThrownBy(() -> listener.onMessage(record, acknowledgment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Message payload is not valid JSON");

        verify(fairTopicBuffer, never()).offer(any());
        verify(acknowledgment, never()).acknowledge();
    }
}
