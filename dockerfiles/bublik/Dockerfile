FROM docker-hosted.artifactory.tcsbank.ru/bigops-sre/infra/ubuntu-openjdk21:latest
ARG JAR

ADD $JAR ./bublik.jar

ENV JAVA_OPTS="  "
ENV BUBLIK_ARGS=""

ENTRYPOINT java $JAVA_OPTS -jar ./bublik.jar $BUBLIK_ARGS