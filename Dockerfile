ARG JAVA_VERSION
FROM openjdk:${JAVA_VERSION}

ENV PATH $JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /kafkasink

RUN apt-get -qq update && \
    apt-get -qqy install apt-transport-https wget util-linux && \
    apt-get clean

COPY build/libs/reproduce-flink-kafkasink-1.0-all.jar jars/reproduce-flink-kafkasink.jar

ENTRYPOINT ["java", "-jar", "jars/reproduce-flink-kafkasink.jar"]