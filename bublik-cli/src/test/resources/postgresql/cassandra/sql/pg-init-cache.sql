-- Таблица-источник TTL кэша)
create table public.offer (
    id bigint,
    open_date timestamp,
    cb_service_name text,
    targeting_type text,
    close_date timestamp,
    primary key (id)
);

create table public.source (
    id bigint,
    offer_id bigint,
    flags int,
    primary key (id)
);


-- Вставка тестовых данных в offer
-- id=12345: HOTELS_POSTPAY - живет 2 года
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date) 
values (12345, '2025-06-04 00:00:00.000 +0300', 'HOTELS_POSTPAY', 'DYNAMIC', '2027-06-04 00:00:00.000 +0300');

-- id=23456: HOTELS_POSTPAY_PREDICTOR - живет 2 года
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date) 
values (23456, '2025-06-04 00:00:00.000 +0300', 'HOTELS_POSTPAY_PREDICTOR', 'DYNAMIC', '2027-06-04 00:00:00.000 +0300');

-- id=34567: AVIA - живет 6 месяцев
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date) 
values (34567, '2025-06-04 00:00:00.000 +0300', 'AVIA', 'DYNAMIC', '2025-12-04 00:00:00.000 +0300');

-- id=45678: CONCERT - живет 6 месяцев
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date) 
values (45678, '2025-06-04 00:00:00.000 +0300', 'CONCERT', 'TRANSACTION', '2025-12-04 00:00:00.000 +0300');

-- id=56789: HEALTH - живет 1 месяц
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date) 
values (56789, '2025-06-04 00:00:00.000 +0300', 'HEALTH', 'TRANSACTION', '2025-07-04 00:00:00.000 +0300');

-- id=77777: EXPIRED - для теста отрицательного TTL
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date) 
values (77777, '2020-01-01 00:00:00.000 +0300', 'HEALTH', 'TRANSACTION', '2020-02-01 00:00:00.000 +0300');

-- Вставка тестовых данных в ttl_check
-- id=1: offer_id=12345 - TTL из кэша (2 года)
insert into public.source (id, offer_id) values (1, 12345);

-- id=2: offer_id=23456 - TTL из кэша (2 года)
insert into public.source (id, offer_id) values (2, 23456);

-- id=3: offer_id=34567 - TTL из кэша (6 месяцев)
insert into public.source (id, offer_id) values (3, 34567);

-- id=4: offer_id=45678 - TTL из кэша (6 месяцев)
insert into public.source (id, offer_id) values (4, 45678);

-- id=5: offer_id=56789 - TTL из кэша (1 месяц)
insert into public.source (id, offer_id) values (5, 56789);

-- id=6: offer_id=99999 - отсутствует в кэше, TTL по умолчанию (2 года)
insert into public.source (id, offer_id) values (6, 99999);

-- id=7: offer_id=77777 - отрицательный TTL, будет заменен на 1 неделю
insert into public.source (id, offer_id) values (7, 77777);


analyze public.source;
analyze public.offer;
