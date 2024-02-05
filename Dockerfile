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
RUN apt-get install -y openjdk-17-jre python3 python3-pip python3-venv
# python virtual environment
RUN python3 -m venv external/venv
RUN external/venv/bin/python3 -m pip install --no-cache-dir --upgrade pip setuptools
# python requirements
COPY external/requirements.txt external/requirements.txt
RUN external/venv/bin/python3 -m pip install --no-cache-dir -r external/requirements.txt
# python bayes script
COPY external/bayes.py external/bayes.py
COPY external/kmeans.py external/kmeans.py
# copy application jar
COPY --from=build /build/target/cws-k8s-scheduler*.jar cws-k8s-scheduler.jar

RUN addgroup --system javagroup && adduser --system javauser --ingroup javagroup
RUN chown -R javauser:javagroup /app
USER javauser
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/cws-k8s-scheduler.jar"]
