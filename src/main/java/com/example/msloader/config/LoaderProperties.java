package com.example.msloader.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.loader")
public class LoaderProperties {

    @NotEmpty
    private List<String> topics = new ArrayList<>();

    @Min(1)
    private int listenerConcurrency = 12;

    @Min(1)
    private int writerBatchSize = 200;

    @Min(10)
    private long writerDelayMs = 250;

    @Min(100)
    private int inMemoryBufferCapacity = 10_000;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public int getListenerConcurrency() {
        return listenerConcurrency;
    }

    public void setListenerConcurrency(int listenerConcurrency) {
        this.listenerConcurrency = listenerConcurrency;
    }

    public int getWriterBatchSize() {
        return writerBatchSize;
    }

    public void setWriterBatchSize(int writerBatchSize) {
        this.writerBatchSize = writerBatchSize;
    }

    public long getWriterDelayMs() {
        return writerDelayMs;
    }

    public void setWriterDelayMs(long writerDelayMs) {
        this.writerDelayMs = writerDelayMs;
    }

    public int getInMemoryBufferCapacity() {
        return inMemoryBufferCapacity;
    }

    public void setInMemoryBufferCapacity(int inMemoryBufferCapacity) {
        this.inMemoryBufferCapacity = inMemoryBufferCapacity;
    }
}
