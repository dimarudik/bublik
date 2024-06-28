FROM maven:3.9.8-amazoncorretto-21-al2023 as bublik
COPY ./bublik/src /usr/src/app/src
COPY ./bublik/pom.xml /usr/src/app

RUN mvn -f /usr/src/app/pom.xml clean install -DskipTests

FROM maven:3.9.8-amazoncorretto-21-al2023 as cli
COPY ./cli/src /usr/src/app/src
COPY ./cli/pom.xml /usr/src/app
COPY --from=bublik /usr/src/app/target/bublik-1.2.0.jar /tmp/bublik-1.2.0.jar

RUN mvn install:install-file -DgroupId=org.bublik -DartifactId=bublik -Dpackaging=jar \
      -Dversion=1.2.0 -Dfile=/tmp/bublik-1.2.0.jar
#      -DgeneratePom=true

RUN mvn -f /usr/src/app/pom.xml clean install -DskipTests

FROM openjdk:24-slim-bullseye

RUN mkdir -p /usr/app/config && mkdir -p /usr/app/target
COPY ./cli/config /usr/app/config
COPY --from=bublik /usr/src/app/target /usr/app/target
COPY --from=cli /usr/src/app/target /usr/app/target

#ENTRYPOINT ["cat", "/usr/app/config/pg2cs.yaml"]
ENTRYPOINT ["java", "-jar", "/usr/app/target/bublik-cli-1.2.0.jar", "-c", "/usr/app/config/pg2cs.yaml", "-m", "/usr/app/config/pg2cs.json"]

#docker build --no-cache -t cli .
#docker run -h cli --ip 172.28.0.5 --network bublik-network --name cli cli:latest
