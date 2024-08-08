FROM cassandra:latest
COPY ./cs-172.28.0.6.yaml /etc/cassandra/cassandra.yaml
COPY ./jvm-server.options /etc/cassandra/jvm-server.options
ENV CASSANDRA_CLUSTER_NAME=bublik
ENV CASSANDRA_SEEDS=172.28.0.6
CMD ["cassandra", "-f"]
