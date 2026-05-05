create table if not exists kafka_messages
(
    id              bigserial primary key,
    topic_name      varchar(255) not null,
    partition_id    integer not null,
    message_offset  bigint not null,
    message_key     text,
    message_value   jsonb not null,
    headers_json    jsonb not null,
    kafka_timestamp timestamp with time zone not null,
    saved_at        timestamp with time zone not null
);

create index if not exists idx_kafka_messages_topic_partition_offset
    on kafka_messages (topic_name, partition_id, message_offset);

create index if not exists idx_kafka_messages_kafka_timestamp
    on kafka_messages (kafka_timestamp);
