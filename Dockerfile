#FROM maven:3.9.8-amazoncorretto-21-al2023 as bublik
#COPY ./bublik/src /usr/src/ydbClient/src
#COPY ./bublik/pom.xml /usr/src/ydbClient

#RUN mvn -f /usr/src/ydbClient/pom.xml clean install -DskipTests

#FROM maven:3.9.8-amazoncorretto-21-al2023 as cli
#COPY ./cli/src /usr/src/ydbClient/src
#COPY ./cli/pom.xml /usr/src/ydbClient
#COPY --from=bublik /usr/src/ydbClient/target/bublik-1.2.0.jar /tmp/bublik-1.2.0.jar

#RUN mvn install:install-file -DgroupId=org.bublik -DartifactId=bublik -Dpackaging=jar -Dversion=1.2.0 -Dfile=/tmp/bublik-1.2.0.jar

#RUN mvn -f /usr/src/ydbClient/pom.xml clean install -DskipTests

FROM openjdk:24-slim-bullseye

RUN mkdir -p /usr/ydbClient/config && mkdir -p /usr/ydbClient/target
COPY ./cli/config /usr/ydbClient/config
COPY ./bublik/target/lib /usr/ydbClient/target/lib
COPY ./bublik/target/bublik-1.2.0.jar /usr/ydbClient/target/lib/bublik-1.2.0.jar
COPY ./cli/target/lib /usr/ydbClient/target/lib
COPY ./cli/target/bublik-cli-1.2.0.jar /usr/ydbClient/target/bublik-cli-1.2.0.jar
#COPY --from=bublik /usr/src/ydbClient/target /usr/ydbClient/target
#COPY --from=cli /usr/src/ydbClient/target /usr/ydbClient/target

#ENTRYPOINT ["cat", "/usr/ydbClient/config/pg2cs.yaml"]
ENTRYPOINT ["java", "-jar", "/usr/ydbClient/target/bublik-cli-1.2.0.jar", "-c", "/usr/ydbClient/config/pg2cs.yaml", "-m", "/usr/ydbClient/config/pg2cs.json"]

# docker rmi cli ; docker build -t cli . ; docker run -h cli --network bublik-network --name cli cli:latest

#docker rmi $(docker images -f "dangling=true" -q)
#docker volume prune -f

# https://support.datastax.com/s/article/Change-CompactionStrategy-and-subproperties-via-JMX
# java -jar jmxsh-R5.jar -h localhost -p 7199

# docker exec cs1 nodetool removenode <id>
# docker build ./k8s -t dimarudik/flights -f ./k8s/Dockerfile-flights --no-cache --progress=plain

