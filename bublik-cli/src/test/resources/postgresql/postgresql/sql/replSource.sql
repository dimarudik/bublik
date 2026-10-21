CREATE TABLE public.replicated_data (
    id BIGSERIAL,
    uuid_key UUID DEFAULT gen_random_uuid() NOT NULL,
    payload_text VARCHAR(256),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    a_id bigint,
    CONSTRAINT pk_replicated_data PRIMARY KEY (id)
);

INSERT INTO public.replicated_data (payload_text)
SELECT 'Historical item #' || g
FROM generate_series(1, 1000000) g;
analyze public.replicated_data;

create table public.a (
    id BIGSERIAL primary key,
    repl_id bigint,
    constraint fk_a_repl_id foreign key (repl_id) references public.replicated_data (id)
);
insert into public.a (repl_id) select id from public.replicated_data limit 10;
analyze public.a;

alter table public.replicated_data add constraint fk_replicated_data_a_id foreign key (a_id) references public.a (id);
update public.replicated_data set a_id = 1 where id = 1;

SELECT pg_create_logical_replication_slot('bublik_slot', 'pgoutput');
CREATE PUBLICATION bublik_pub FOR TABLE public.replicated_data;
