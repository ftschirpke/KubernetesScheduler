FROM maven:3-openjdk-17-slim AS build
WORKDIR /build
COPY pom.xml pom.xml
RUN mvn dependency:go-offline --no-transfer-progress -Dmaven.repo.local=/mvn/.m2nrepo/repository
COPY src/ src/
RUN mvn package --no-transfer-progress -DskipTests -Dmaven.repo.local=/mvn/.m2nrepo/repository

#
# Package stage
#
FROM openjdk:17-alpine
WORKDIR /app
RUN addgroup -S javagroup && adduser -S javauser -G javagroup && mkdir data
COPY --from=build /build/target/cws-k8s-scheduler*.jar cws-k8s-scheduler.jar

# install python3 for the external scripts
COPY external/* external/
RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools
RUN pip3 install -r external/requirements.txt
RUN chown -R javauser:javagroup /app

USER javauser
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/cws-k8s-scheduler.jar"]
