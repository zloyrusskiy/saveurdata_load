CREATE EXTENSION IF NOT EXISTS citus;

CREATE TABLE saveurdata
(
    phone bigint not null,
    data  jsonb  not null
)
    USING columnar;

create table saveurdata_load_status
(
    phone_prefix char(8)  not null
        constraint saveurdata_load_status_pk
            primary key,
    http_status  smallint not null
);
