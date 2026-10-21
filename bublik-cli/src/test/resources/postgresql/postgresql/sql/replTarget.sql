CREATE TABLE public.replicated_data (
    id BIGSERIAL,
    uuid_key UUID NOT NULL,
    payload_text VARCHAR(256),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    a_id bigint,
    CONSTRAINT pk_replicated_data PRIMARY KEY (id)
);

create table public.a (
    id BIGSERIAL primary key,
    repl_id bigint,
    constraint fk_a_repl_id foreign key (repl_id) references public.replicated_data (id)
);
alter table public.replicated_data add constraint fk_replicated_data_a_id foreign key (a_id) references public.a (id);

CREATE SUBSCRIPTION bublik_sub
CONNECTION 'host=source port=5432 dbname=postgres user=test password=test'
PUBLICATION bublik_pub
WITH (
    enabled = false,
    create_slot = false,
    copy_data = false,
    slot_name = 'bublik_slot'
);
