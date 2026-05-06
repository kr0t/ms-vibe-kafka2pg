package com.example.msloader.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.msloader.config.LoaderProperties;
import com.example.msloader.domain.BufferedKafkaMessage;
import com.example.msloader.domain.KafkaMessageEntity;
import com.example.msloader.repository.KafkaMessageRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KafkaMessagePersistenceServiceTest {

    private final FairTopicBuffer fairTopicBuffer = mock(FairTopicBuffer.class);
    private final KafkaMessageRepository repository = mock(KafkaMessageRepository.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldSkipPersistenceWhenBatchIsEmpty() {
        LoaderProperties properties = properties();
        when(fairTopicBuffer.drainFairBatch(anyInt())).thenReturn(List.of());
        KafkaMessagePersistenceService service =
                new KafkaMessagePersistenceService(fairTopicBuffer, repository, properties, objectMapper);

        service.persistBufferedMessages();

        verify(repository, never()).saveAll(any());
    }

    @Test
    void shouldMapBufferedMessagesToDatabaseEntities() {
        LoaderProperties properties = properties();
        KafkaMessagePersistenceService service =
                new KafkaMessagePersistenceService(fairTopicBuffer, repository, properties, objectMapper);

        BufferedKafkaMessage later = message("topic-2", "key-2", 20, 2);
        BufferedKafkaMessage earlier = message("topic-1", null, 10, 1);
        when(fairTopicBuffer.drainFairBatch(50)).thenReturn(List.of(later, earlier));

        service.persistBufferedMessages();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<KafkaMessageEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(repository).saveAll(captor.capture());

        List<KafkaMessageEntity> saved = captor.getValue();
        assertThat(saved).hasSize(2);
        assertThat(saved.get(0).getTopic()).isEqualTo("topic-1");
        assertThat(saved.get(0).getMsgKey()).isEmpty();
        assertThat(saved.get(0).getValueJson()).contains("\"index\":1");
        assertThat(saved.get(0).getHeadersIson()).contains("\"header-1\"");
        assertThat(saved.get(0).getQueryType()).isEqualTo("LOAD");
        assertThat(saved.get(0).getStatus()).isEqualTo("NEW");
        assertThat(saved.get(0).getNamespace()).isEqualTo("test-namespace");
        assertThat(saved.get(0).getPodName()).isEqualTo("test-pod");
        assertThat(saved.get(0).getDescription()).isEqualTo("test description");
        assertThat(saved.get(0).getKafkaDttm()).isEqualTo(Instant.parse("2026-05-06T00:00:01Z"));
        assertThat(saved.get(0).getStartDttm()).isEqualTo(Instant.parse("2026-05-06T00:00:01.100Z"));
        assertThat(saved.get(0).getCompleteDttm()).isNotNull();
        assertThat(saved.get(1).getTopic()).isEqualTo("topic-2");
        assertThat(saved.get(1).getMsgKey()).isEqualTo("key-2");
    }

    @Test
    void shouldWrapSerializationErrors() throws Exception {
        LoaderProperties properties = properties();
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        KafkaMessagePersistenceService service =
                new KafkaMessagePersistenceService(fairTopicBuffer, repository, properties, failingMapper);
        BufferedKafkaMessage message = message("topic-1", "key-1", 10, 1);
        when(fairTopicBuffer.drainFairBatch(50)).thenReturn(List.of(message));
        when(failingMapper.writeValueAsString(any()))
                .thenThrow(new JsonProcessingException("boom") { });

        assertThatThrownBy(service::persistBufferedMessages)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot serialize message payload for database");
    }

    private LoaderProperties properties() {
        LoaderProperties properties = new LoaderProperties();
        properties.setWriterBatchSize(50);
        properties.setQueryType("LOAD");
        properties.setStatus("NEW");
        properties.setNamespace("test-namespace");
        properties.setPodName("test-pod");
        properties.setDescription("test description");
        return properties;
    }

    private BufferedKafkaMessage message(String topic, String key, int offset, int index) {
        Instant kafkaTime = Instant.parse("2026-05-06T00:00:0%dZ".formatted(index));
        Instant bufferedAt = Instant.parse("2026-05-06T00:00:0%d.100Z".formatted(index));
        return new BufferedKafkaMessage(
                topic,
                index,
                offset,
                key,
                objectMapper.createObjectNode().put("index", index),
                objectMapper.createArrayNode().add("header-" + index),
                kafkaTime,
                bufferedAt
        );
    }
}
