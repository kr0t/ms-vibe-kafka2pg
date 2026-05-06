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

    private String queryType = "LOAD";

    private String status = "NEW";

    private String namespace = "local";

    private String podName = "local-pod";

    private String description;

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

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
