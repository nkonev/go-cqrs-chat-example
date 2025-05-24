with normalized_user as (
                        select unnest(cast (array[1] as bigint[])) as user_id
                ),
                last_message as (
                        select 
                                coalesce(ww.last_message_id, 0) as last_message_id,
                                nu.user_id
                        from (
                                select
                                        (case
                                                when exists(select * from unread_messages_user_view uw where uw.chat_id = 1 and uw.user_id = w.user_id and uw.last_message_id > 0)
                                                then coalesce(
                                                        (select id as last_message_id from message m where m.chat_id = 1 and m.id = w.last_message_id),
                                                        (select max(id) as max from message m where m.chat_id = 1 and false = true)
                                                )
                                        end) as last_message_id,
                                        w.user_id
                                from unread_messages_user_view w 
                                where w.chat_id = 1 and w.user_id = any(array[1])
                        ) ww
                        right join normalized_user nu on ww.user_id = nu.user_id
                ),
                existing_message as (
                        select coalesce(
                                (select id from message where chat_id = 1 and id = 0),
                                (select max(id) as max from message where chat_id = 1),
                                0
                        ) as normalized_message_id
                ),
                normalized_given_message as (
                        select 
                                n.user_id,
                                (case 
                                        when true = true then (select l.last_message_id from last_message l where l.user_id = n.user_id)
                                        else (select normalized_message_id from existing_message) 
                                end) as normalized_message_id
                        from normalized_user n
                ),
                input_data as (
                        select
                                ngm.user_id as user_id,
                                cast (1 as bigint) as chat_id,
                                (SELECT count(m.id) FILTER(WHERE m.id > (select normalized_message_id from normalized_given_message n where n.user_id = ngm.user_id))
                                        FROM message m
                                        WHERE m.chat_id = 1) as unread_messages,
                                ngm.normalized_message_id as last_message_id
                        from normalized_given_message ngm
                )
                insert into unread_messages_user_view(user_id, chat_id, unread_messages, last_message_id)
                select 
                        idt.user_id,
                        idt.chat_id,
                        idt.unread_messages,
                        idt.last_message_id
                from input_data idt
                on conflict (user_id, chat_id) do update set unread_messages = excluded.unread_messages, last_message_id = excluded.last_message_id
                ;
