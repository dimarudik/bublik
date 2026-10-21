CREATE TABLE public.replicated_data (
    id BIGSERIAL,
    uuid_key UUID DEFAULT gen_random_uuid() NOT NULL,
    payload_text VARCHAR(256),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT pk_replicated_data PRIMARY KEY (id)
);

INSERT INTO public.replicated_data (payload_text)
SELECT 'Historical item #' || g
FROM generate_series(1, 1000000) g;
analyze public.replicated_data;

SELECT pg_create_logical_replication_slot('bublik_slot', 'pgoutput');
CREATE PUBLICATION bublik_pub FOR TABLE public.replicated_data;

