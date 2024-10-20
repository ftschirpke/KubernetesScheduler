FROM maven:3-openjdk-17-slim AS build
WORKDIR /build
COPY pom.xml pom.xml
RUN mvn dependency:go-offline --no-transfer-progress -Dmaven.repo.local=/mvn/.m2nrepo/repository
COPY src/ src/
RUN mvn package --no-transfer-progress -DskipTests -Dmaven.repo.local=/mvn/.m2nrepo/repository

#
# Package stage
#
FROM debian:bullseye-slim
WORKDIR /app

# install java runtime, python3 and pip
RUN apt-get update
RUN apt-get install -y openjdk-17-jre
# copy application jar
COPY --from=build /build/target/cws-k8s-scheduler*.jar cws-k8s-scheduler.jar

RUN addgroup --system javagroup && adduser --system javauser --ingroup javagroup
RUN chown -R javauser:javagroup /app
USER javauser
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/cws-k8s-scheduler.jar"]
