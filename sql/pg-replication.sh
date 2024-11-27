docker network list
docker network create \
  --driver=bridge \
  --subnet=172.28.0.0/16 \
  --gateway=172.28.5.254 \
  bublik-network

docker run \
        --name pg1 \
        --ip 172.28.0.7 \
        -h pg1 \
        --network bublik-network \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432:5432 \
        -v ./sql/init-full.sql:/docker-entrypoint-initdb.d/init-full.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -v ./sql/bublik.png:/var/lib/postgresql/bublik.png \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain" \
        -c timezone="+03" \
        -c max_connections=200 \
        -c logging_collector=on \
        -c log_directory=pg_log \
        -c log_filename=%u_%a.log \
        -c log_min_duration_statement=10 \
        -c log_statement=ddl \
        -c auto_explain.log_min_duration=10 \
        -c auto_explain.log_analyze=true \
        -c wal_level=logical

docker run \
        --name pg2 \
        --ip 172.28.0.17 \
        -h pg2 \
        --network bublik-network \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5433:5432 \
        -v ./sql/init-empty.sql:/docker-entrypoint-initdb.d/init-empty.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain" \
        -c timezone="+03" \
        -c max_connections=200 \
        -c logging_collector=on \
        -c log_directory=pg_log \
        -c log_filename=%u_%a.log \
        -c log_min_duration_statement=10 \
        -c log_statement=ddl \
        -c auto_explain.log_min_duration=10 \
        -c auto_explain.log_analyze=true \
        -c wal_level=logical \
        -c tcp_keepalives_idle=60 \
        -c tcp_keepalives_interval=3 \
        -c tcp_keepalives_count=3

docker run \
        --name pg3 \
        --ip 172.28.0.27 \
        -h pg3 \
        --network bublik-network \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5434:5432 \
        -v ./sql/init-empty.sql:/docker-entrypoint-initdb.d/init-empty.sql \
        -v ./sql/.psqlrc:/var/lib/postgresql/.psqlrc \
        -d postgres \
        -c shared_preload_libraries="pg_stat_statements,auto_explain" \
        -c timezone="+03" \
        -c max_connections=200 \
        -c logging_collector=on \
        -c log_directory=pg_log \
        -c log_filename=%u_%a.log \
        -c log_min_duration_statement=10 \
        -c log_statement=ddl \
        -c auto_explain.log_min_duration=10 \
        -c auto_explain.log_analyze=true \
        -c wal_level=logical

# docker network disconnect bublik-network pg3

create publication pub;
alter publication pub add table users;
alter publication pub add table items;
alter publication pub add table likes;

create subscription sub connection 'postgresql://test:test@pg1:5432/postgres' publication pub with (copy_data=true, create_slot=true, slot_name='pub', disable_on_error = false);
alter subscription sub refresh publication;

begin;
insert into users values (100001, 'q', 'q');
insert into items values (100001, 'q', 'q');
insert into likes values (10000001, 100001, 100001);
