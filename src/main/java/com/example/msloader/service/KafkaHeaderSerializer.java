package com.example.msloader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

@Component
public class KafkaHeaderSerializer {

    private final ObjectMapper objectMapper;

    public KafkaHeaderSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JsonNode toJson(Headers headers) {
        List<Map<String, Object>> serialized = new ArrayList<>();
        for (Header header : headers) {
            byte[] value = header.value();
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("key", header.key());
            item.put("valueBase64", value == null ? "" : Base64.getEncoder().encodeToString(value));
            item.put("valueUtf8", tryUtf8(value));
            serialized.add(item);
        }
        return objectMapper.valueToTree(serialized);
    }

    private String tryUtf8(byte[] value) {
        if (value == null || value.length == 0) {
            return "";
        }
        return new String(value, StandardCharsets.UTF_8);
    }
}
