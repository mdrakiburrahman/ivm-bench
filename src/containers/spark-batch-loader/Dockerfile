ARG SBT_IMAGE=sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.6_2.12.21
FROM ${SBT_IMAGE} AS builder
ARG SPARK_VERSION=3.5.8
ARG SCALA_VERSION=2.12.18
ARG DELTA_VERSION=3.3.2
ARG HADOOP_VERSION=3.3.4

WORKDIR /app
COPY build.sbt .
COPY project/ project/
RUN sbt -Dspark.version=$SPARK_VERSION \
        -Dscala.version=$SCALA_VERSION \
        -Ddelta.version=$DELTA_VERSION \
        -Dhadoop.version=$HADOOP_VERSION \
        update

COPY src/ src/
RUN sbt -Dspark.version=$SPARK_VERSION \
        -Dscala.version=$SCALA_VERSION \
        -Ddelta.version=$DELTA_VERSION \
        -Dhadoop.version=$HADOOP_VERSION \
        assembly

FROM eclipse-temurin:17-jre

COPY --from=builder /app/target/scala-*/batch-loader-assembly.jar /opt/batch-loader.jar
COPY log4j2.properties /opt/log4j2.properties
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
