create table blog(
    id int primary key,
    owner_id bigint,
    title varchar(256) not null,
    post text,
    preview varchar(512),
    create_date_time timestamp not null
);
