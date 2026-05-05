package com.example.msloader;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.msloader.domain.KafkaMessageEntity;
import com.example.msloader.repository.KafkaMessageRepository;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.lifecycle.Startables;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaToPostgresIntegrationTest {

    private static final List<String> TOPICS = List.of("topic-1", "topic-2", "topic-3", "topic-4");
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:3.9.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE);

    static {
        Startables.deepStart(POSTGRESQL, KAFKA).join();
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaMessageRepository kafkaMessageRepository;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRESQL::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRESQL::getUsername);
        registry.add("spring.datasource.password", POSTGRESQL::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.producer.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.producer.value-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.consumer.group-id", () -> "ms-spring-loader-it");
        registry.add("app.loader.topics", () -> String.join(",", TOPICS));
        registry.add("app.loader.listener-concurrency", () -> 12);
        registry.add("app.loader.writer-batch-size", () -> 20);
        registry.add("app.loader.writer-delay-ms", () -> 100);
    }

    @BeforeAll
    void createTopics() throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()
        ))) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<NewTopic> topicsToCreate = TOPICS.stream()
                    .filter(topic -> !existingTopics.contains(topic))
                    .map(topic -> new NewTopic(topic, 3, (short) 1))
                    .toList();

            for (NewTopic topic : topicsToCreate) {
                try {
                    adminClient.createTopics(List.of(topic)).all().get();
                } catch (ExecutionException exception) {
                    if (!(exception.getCause() instanceof TopicExistsException)) {
                        throw exception;
                    }
                }
            }
        }
    }

    @Test
    void shouldConsumeFromAllTopicsAndPersistMessagesToPostgres() {
        kafkaMessageRepository.deleteAll();

        for (int i = 0; i < TOPICS.size(); i++) {
            String topic = TOPICS.get(i);
            String payload = """
                    {"topic":"%s","index":%d}
                    """.formatted(topic, i + 1);

            kafkaTemplate.send(topic, "key-" + (i + 1), payload);
        }

        kafkaTemplate.flush();

        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    List<KafkaMessageEntity> messages = kafkaMessageRepository.findAll();
                    assertThat(messages).hasSize(4);

                    Set<String> persistedTopics = messages.stream()
                            .map(KafkaMessageEntity::getTopicName)
                            .collect(Collectors.toSet());

                    assertThat(persistedTopics).containsExactlyInAnyOrderElementsOf(TOPICS);
                    assertThat(messages)
                            .allSatisfy(message -> {
                                assertThat(message.getMessageKey()).startsWith("key-");
                                assertThat(message.getHeadersJson()).isNotNull();
                                assertThat(message.getMessageValue()).isNotNull();
                                assertThat(message.getKafkaTimestamp()).isNotNull();
                                assertThat(message.getSavedAt()).isNotNull();
                            });
                });
    }
}
