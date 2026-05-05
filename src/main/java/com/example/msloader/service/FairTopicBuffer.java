package com.example.msloader.service;

import com.example.msloader.config.LoaderProperties;
import com.example.msloader.domain.BufferedKafkaMessage;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class FairTopicBuffer {

    private final LoaderProperties loaderProperties;
    private final AtomicInteger cursor = new AtomicInteger();
    private final Map<String, BlockingQueue<BufferedKafkaMessage>> queuesByTopic = new LinkedHashMap<>();
    private List<String> topics;

    public FairTopicBuffer(LoaderProperties loaderProperties) {
        this.loaderProperties = loaderProperties;
    }

    @PostConstruct
    void init() {
        this.topics = List.copyOf(loaderProperties.getTopics());
        int queueCapacityPerTopic = Math.max(1, loaderProperties.getInMemoryBufferCapacity() / topics.size());
        for (String topic : topics) {
            queuesByTopic.put(topic, new LinkedBlockingQueue<>(queueCapacityPerTopic));
        }
    }

    public void offer(BufferedKafkaMessage message) throws InterruptedException {
        BlockingQueue<BufferedKafkaMessage> queue = queuesByTopic.get(message.topicName());
        if (queue == null) {
            throw new IllegalArgumentException("Topic is not configured: " + message.topicName());
        }
        queue.put(message);
    }

    public List<BufferedKafkaMessage> drainFairBatch(int maxBatchSize) {
        List<BufferedKafkaMessage> batch = new ArrayList<>(maxBatchSize);
        if (topics.isEmpty()) {
            return batch;
        }

        int startIndex = Math.floorMod(cursor.getAndIncrement(), topics.size());
        int currentIndex = startIndex;

        while (batch.size() < maxBatchSize) {
            boolean drainedAny = false;

            for (int checked = 0; checked < topics.size() && batch.size() < maxBatchSize; checked++) {
                String topic = topics.get(currentIndex);
                BufferedKafkaMessage message = queuesByTopic.get(topic).poll();
                if (message != null) {
                    batch.add(message);
                    drainedAny = true;
                }
                currentIndex = (currentIndex + 1) % topics.size();
            }

            if (!drainedAny) {
                break;
            }
        }

        return batch;
    }

    public int size() {
        return queuesByTopic.values().stream().mapToInt(BlockingQueue::size).sum();
    }
}
