package com.example.msloader.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.msloader.config.LoaderProperties;
import com.example.msloader.domain.BufferedKafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class FairTopicBufferTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldDrainMessagesInRoundRobinOrderAcrossTopics() throws Exception {
        FairTopicBuffer buffer = createBuffer(List.of("topic-1", "topic-2", "topic-3"));

        buffer.offer(message("topic-1", 1));
        buffer.offer(message("topic-1", 2));
        buffer.offer(message("topic-2", 3));
        buffer.offer(message("topic-2", 4));
        buffer.offer(message("topic-3", 5));
        buffer.offer(message("topic-3", 6));

        List<BufferedKafkaMessage> batch = buffer.drainFairBatch(6);

        assertThat(batch)
                .extracting(BufferedKafkaMessage::topicName)
                .containsExactly("topic-1", "topic-2", "topic-3", "topic-1", "topic-2", "topic-3");
    }

    @Test
    void shouldNotDrainSingleTopicCompletelyBeforeOthers() throws Exception {
        FairTopicBuffer buffer = createBuffer(List.of("topic-1", "topic-2", "topic-3", "topic-4"));

        buffer.offer(message("topic-1", 1));
        buffer.offer(message("topic-1", 2));
        buffer.offer(message("topic-1", 3));
        buffer.offer(message("topic-1", 4));
        buffer.offer(message("topic-2", 5));
        buffer.offer(message("topic-3", 6));
        buffer.offer(message("topic-4", 7));

        List<BufferedKafkaMessage> batch = buffer.drainFairBatch(5);

        assertThat(batch)
                .extracting(BufferedKafkaMessage::topicName)
                .containsExactly("topic-1", "topic-2", "topic-3", "topic-4", "topic-1");
    }

    private FairTopicBuffer createBuffer(List<String> topics) {
        LoaderProperties properties = new LoaderProperties();
        properties.setTopics(topics);
        properties.setInMemoryBufferCapacity(100);

        FairTopicBuffer buffer = new FairTopicBuffer(properties);
        buffer.init();
        return buffer;
    }

    private BufferedKafkaMessage message(String topic, int index) {
        Instant now = Instant.parse("2026-05-05T00:00:00Z").plusSeconds(index);
        return new BufferedKafkaMessage(
                topic,
                0,
                index,
                "key-" + index,
                objectMapper.createObjectNode().put("index", index),
                objectMapper.createArrayNode(),
                now,
                now
        );
    }
}
