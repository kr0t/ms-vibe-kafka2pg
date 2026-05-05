package com.example.msloader.domain;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "kafka_messages")
public class KafkaMessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "topic_name", nullable = false, length = 255)
    private String topicName;

    @Column(name = "partition_id", nullable = false)
    private int partitionId;

    @Column(name = "message_offset", nullable = false)
    private long messageOffset;

    @Column(name = "message_key")
    private String messageKey;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "message_value", nullable = false, columnDefinition = "jsonb")
    private JsonNode messageValue;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "headers_json", nullable = false, columnDefinition = "jsonb")
    private JsonNode headersJson;

    @Column(name = "kafka_timestamp", nullable = false)
    private Instant kafkaTimestamp;

    @Column(name = "saved_at", nullable = false)
    private Instant savedAt;

    public Long getId() {
        return id;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public long getMessageOffset() {
        return messageOffset;
    }

    public void setMessageOffset(long messageOffset) {
        this.messageOffset = messageOffset;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public JsonNode getMessageValue() {
        return messageValue;
    }

    public void setMessageValue(JsonNode messageValue) {
        this.messageValue = messageValue;
    }

    public JsonNode getHeadersJson() {
        return headersJson;
    }

    public void setHeadersJson(JsonNode headersJson) {
        this.headersJson = headersJson;
    }

    public Instant getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public void setKafkaTimestamp(Instant kafkaTimestamp) {
        this.kafkaTimestamp = kafkaTimestamp;
    }

    public Instant getSavedAt() {
        return savedAt;
    }

    public void setSavedAt(Instant savedAt) {
        this.savedAt = savedAt;
    }
}
