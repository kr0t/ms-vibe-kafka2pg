package com.example.msloader.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildConsumerProperties());
        properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            LoaderProperties loaderProperties) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(loaderProperties.getListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setBatchListener(false);
        return factory;
    }
}
