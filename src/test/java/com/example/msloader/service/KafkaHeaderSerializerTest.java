package com.example.msloader.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class KafkaHeaderSerializerTest {

    private final KafkaHeaderSerializer serializer = new KafkaHeaderSerializer(new ObjectMapper());

    @Test
    void shouldSerializeHeadersToJsonTree() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("source", "unit-test".getBytes(StandardCharsets.UTF_8));
        headers.add("binary", new byte[]{1, 2, 3});

        JsonNode result = serializer.toJson(headers);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).get("key").asText()).isEqualTo("source");
        assertThat(result.get(0).get("valueUtf8").asText()).isEqualTo("unit-test");
        assertThat(result.get(1).get("key").asText()).isEqualTo("binary");
        assertThat(result.get(1).get("valueBase64").asText()).isEqualTo("AQID");
    }

    @Test
    void shouldSerializeNullHeaderValueAsEmptyStrings() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("nullable", null);

        JsonNode result = serializer.toJson(headers);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).get("valueBase64").asText()).isEmpty();
        assertThat(result.get(0).get("valueUtf8").asText()).isEmpty();
    }
}
