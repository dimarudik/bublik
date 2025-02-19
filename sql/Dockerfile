FROM postgres:latest
USER root:root
RUN apt-get update -y && apt-get -y install wget curl vim less lsof bsdmainutils binutils git git-lfs procps make build-essential libpq-dev dnsutils postgresql-server-dev-16
RUN unlink /etc/localtime && ln -s /usr/share/zoneinfo/Europe/Moscow /etc/localtime
RUN echo "postgres ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
#docker build ./sql -t pg-dev --no-cache --progress=plain
#gcc my.c -o my -I /usr/include/postgresql -L /usr/lib/postgresql/16/lib -lpq