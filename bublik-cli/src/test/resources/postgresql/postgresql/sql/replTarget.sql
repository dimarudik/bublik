CREATE TABLE public.replicated_data (
    id BIGSERIAL,
    uuid_key UUID NOT NULL,
    payload_text VARCHAR(256),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_replicated_data PRIMARY KEY (id)
);

CREATE SUBSCRIPTION bublik_sub
CONNECTION 'host=source port=5432 dbname=postgres user=test password=test'
PUBLICATION bublik_pub
WITH (
    enabled = false,
    create_slot = false,
    copy_data = false,
    slot_name = 'bublik_slot'
);
