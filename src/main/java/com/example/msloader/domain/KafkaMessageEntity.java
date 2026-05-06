package com.example.msloader.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "tech_messages", schema = "ekld")
public class KafkaMessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "tech_messages_seq")
    @SequenceGenerator(name = "tech_messages_seq", sequenceName = "ekld.tech_messages_seq", allocationSize = 1)
    private Long id;

    @Column(name = "msg_key", nullable = false)
    private String msgKey;

    @Column(name = "value_json", nullable = false, columnDefinition = "text")
    private String valueJson;

    @Column(name = "headers_ison", nullable = false, columnDefinition = "text")
    private String headersIson;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "query_type", nullable = false)
    private String queryType;

    @Column(name = "status", nullable = false, length = 20)
    private String status;

    @Column(name = "kafka_dttm", nullable = false)
    private Instant kafkaDttm;

    @Column(name = "start_dttm")
    private Instant startDttm;

    @Column(name = "complete_dttm")
    private Instant completeDttm;

    @Column(name = "locked_at")
    private Instant lockedAt;

    @Column(name = "namespace")
    private String namespace;

    @Column(name = "pod_name")
    private String podName;

    @Column(name = "description")
    private String description;

    public Long getId() {
        return id;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public String getValueJson() {
        return valueJson;
    }

    public void setValueJson(String valueJson) {
        this.valueJson = valueJson;
    }

    public String getHeadersIson() {
        return headersIson;
    }

    public void setHeadersIson(String headersIson) {
        this.headersIson = headersIson;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public Instant getKafkaDttm() {
        return kafkaDttm;
    }

    public void setKafkaDttm(Instant kafkaDttm) {
        this.kafkaDttm = kafkaDttm;
    }

    public Instant getStartDttm() {
        return startDttm;
    }

    public void setStartDttm(Instant startDttm) {
        this.startDttm = startDttm;
    }

    public Instant getCompleteDttm() {
        return completeDttm;
    }

    public void setCompleteDttm(Instant completeDttm) {
        this.completeDttm = completeDttm;
    }

    public Instant getLockedAt() {
        return lockedAt;
    }

    public void setLockedAt(Instant lockedAt) {
        this.lockedAt = lockedAt;
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
