create sequence chat_id_sequence;

create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    last_generated_message_id bigint not null default 0,
    created_timestamp timestamp not null
);

create table chat_participant(
    user_id bigint not null,
    chat_id bigint not null,
    primary key(user_id, chat_id)
);
SELECT create_distributed_table('chat_participant', 'chat_id');

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

create table chat_user_view(
    id bigint not null,
    title varchar(512) not null,
    pinned boolean not null default false,
    user_id bigint not null,
    updated_timestamp timestamp not null,
    last_message_id bigint,
    last_message_content text,
    last_message_owner_id bigint,
    primary key (user_id, id)
);
SELECT create_distributed_table('chat_user_view', 'user_id');
