create sequence chat_id_sequence;

-- partition by chat_id
create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    last_generated_message_id bigint not null default 0,
    created_timestamp timestamp not null
);

-- partition by chat_id
create table chat_participant(
    user_id bigint not null,
    chat_id bigint not null,
    primary key(user_id, chat_id)
);

-- partition by chat_id
create table message(
    id bigint not null,
    chat_id bigint not null,
    owner_id bigint not null,
    content text not null,
    created_timestamp timestamp not null,
    updated_timestamp timestamp,
    primary key (chat_id, id)
);
SELECT create_distributed_table('message', 'chat_id');
