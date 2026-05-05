# ms-spring-loader

Микросервис на Spring Boot / Java 17, который:

- читает 4 Kafka topic в параллельном режиме;
- сохраняет в PostgreSQL key, value, headers, topic, partition, offset и Kafka timestamp;
- пишет в БД через общий fair-buffer, чтобы сообщения не сохранялись перекошенно по одному topic.

## Как устроено

1. `@KafkaListener` читает сообщения из всех настроенных topic.
2. Параллелизм обеспечивается `listener-concurrency: 12`, что соответствует 4 topic x 3 partition.
3. Все сообщения попадают в `FairTopicBuffer`, где для каждого topic есть собственная очередь.
4. `KafkaMessagePersistenceService` по расписанию забирает batch по round-robin между topic и только потом сохраняет его в PostgreSQL.
5. Перед `saveAll(...)` batch дополнительно сортируется по `kafkaTimestamp`, чтобы внутри выбранной пачки порядок был ближе к реальному времени Kafka.

## Почему запись fair

Если один topic дает большой поток, а другой приходит реже, прямой `save` из listener'а быстро приведет к ситуации, когда в таблице долго будут появляться записи только из одного topic.

Здесь запись отделена от чтения:

- чтение Kafka остается параллельным;
- запись идет через общий буфер;
- при формировании batch берется не весь хвост одного topic, а по кругу из всех непустых topic.

Это не глобальный строгий total order между topic, но это надежно убирает перекос, когда один topic постоянно записывается раньше остальных только потому, что его consumer успел первым.

## Конфигурация

Основные настройки находятся в `src/main/resources/application.yml`:

- `spring.kafka.bootstrap-servers`
- `spring.datasource.*`
- `app.loader.topics` в формате `topic-1,topic-2,topic-3,topic-4`
- `app.loader.listener-concurrency`
- `app.loader.writer-batch-size`
- `app.loader.writer-delay-ms`
- `app.loader.in-memory-buffer-capacity`

## Локальный запуск через Docker Compose

Поднимаются два основных сервиса:

- `postgres` на `localhost:5432`
- `kafka` на `localhost:9092`

Запуск:

```bash
docker compose up -d
```

Если Maven локально не установлен, используйте wrapper из репозитория:

```bash
./mvnw clean test
```

После старта одноразовый контейнер `kafka-init` создаст 4 topic:

- `topic-1`
- `topic-2`
- `topic-3`
- `topic-4`

Для локального запуска приложения укажите:

- `spring.datasource.url=jdbc:postgresql://localhost:5432/messages`
- `spring.kafka.bootstrap-servers=localhost:9092`

## Таблица

DDL лежит в `src/main/resources/schema.sql`.

Поля таблицы:

- `topic_name`
- `partition_id`
- `message_offset`
- `message_key`
- `message_value`
- `headers_json`
- `kafka_timestamp`
- `saved_at`

## Что можно улучшить дальше

- перенести подтверждение Kafka offset на этап после успешной записи в БД, если нужен более строгий сценарий доставки;
- добавить Flyway/Liquibase вместо `schema.sql`;
- вынести dead-letter стратегию на случай невалидного JSON.

## Интеграционный тест

Добавлен integration test на Testcontainers, который:

- поднимает Kafka и PostgreSQL в контейнерах;
- создает 4 topic по 3 partition;
- отправляет тестовые JSON-сообщения;
- проверяет, что сервис сохранил их в PostgreSQL.

Запуск:

```bash
./mvnw test
```

Если Docker недоступен, integration test будет пропущен, но unit test для `FairTopicBuffer` все равно выполнится и проверит round-robin поведение буфера без контейнеров.

Для Colima перед запуском integration test обычно нужно явно указать Docker socket:

```bash
DOCKER_HOST=unix://${HOME}/.colima/default/docker.sock \
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
./mvnw -Dtest=KafkaToPostgresIntegrationTest test
```
