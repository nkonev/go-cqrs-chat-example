create table blog(
    id int primary key,
    owner_id bigint,
    title varchar(256) not null,
    preview varchar(512)
);
