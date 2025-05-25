-- partition by user_id
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
