create schema if not exists ekld;

create sequence if not exists ekld.tech_messages_seq start with 1 increment by 1;

create table if not exists ekld.tech_messages
(
    id            int8 primary key default nextval('ekld.tech_messages_seq'),
    msg_key       text not null,
    value_json    text not null,
    headers_ison  text not null,
    topic         text not null,
    query_type    text not null,
    status        varchar(20) not null,
    kafka_dttm    timestamp(3) with time zone not null,
    start_dttm    timestamp(3) with time zone null,
    complete_dttm timestamp(3) with time zone null,
    locked_at     timestamp(3) with time zone null,
    namespace     text null,
    pod_name      text null,
    description   text null
);

create index if not exists idx_tech_messages_topic
    on ekld.tech_messages (topic);

create index if not exists idx_tech_messages_kafka_dttm
    on ekld.tech_messages (kafka_dttm);
