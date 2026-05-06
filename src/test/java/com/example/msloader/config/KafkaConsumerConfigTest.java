package com.example.msloader.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    void shouldCreateConsumerFactoryWithExpectedDefaults() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.setBootstrapServers(List.of("127.0.0.1:9092"));
        kafkaProperties.getConsumer().setGroupId("group-1");

        ConsumerFactory<String, String> consumerFactory = config.consumerFactory(kafkaProperties);

        Map<String, Object> properties = consumerFactory.getConfigurationProperties();
        assertThat(properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(500);
        assertThat(properties.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("group-1");
    }

    @Test
    void shouldCreateListenerFactoryWithManualAckAndConfiguredConcurrency() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.setBootstrapServers(List.of("127.0.0.1:9092"));
        kafkaProperties.getConsumer().setGroupId("group-1");
        LoaderProperties loaderProperties = new LoaderProperties();
        loaderProperties.setListenerConcurrency(7);

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                config.kafkaListenerContainerFactory(config.consumerFactory(kafkaProperties), loaderProperties);
        ConcurrentMessageListenerContainer<String, String> container = factory.createContainer("topic-1");

        assertThat(container.getConcurrency()).isEqualTo(7);
        assertThat(factory.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL);
        assertThat(factory.isBatchListener()).isFalse();
    }
}
