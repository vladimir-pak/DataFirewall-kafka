create table if not exists df_meta.datafirewall_journal (
    event_id varchar(255),
    action_type varchar(255) not null,
    action_dttm timestamptz not null,
    kafka_timestamp bigint not null,
    kafka_partition varchar(50) not null,
    kafka_offset bigint not null,
    data_json jsonb,
    created_at timestamptz not null default now()
) partition by range (action_dttm);

create table if not exists df_meta.datafirewall_journal_default
partition of df_meta.datafirewall_journal default;
