create table unread_messages_user_view(
    user_id bigint not null,
    chat_id bigint not null,
    unread_messages bigint not null default 0,
    last_message_id bigint not null default 0,
    primary key (user_id, chat_id)
);
SELECT create_distributed_table('unread_messages_user_view', 'user_id');

create table technical(
    id int primary key,
    need_to_fast_forward_sequences bool not null default false
);

