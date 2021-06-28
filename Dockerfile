FROM quay.io/strimzi/kafka:0.21.1-kafka-2.7.0
USER root:root
COPY ./target/Mqtt-kafka-source-connector-1.0-SNAPSHOT.jar /opt/kafka/plugins/
COPY ./target/Mqtt-kafka-source-connector-1.0-SNAPSHOT-package/ /opt/kafka/plugins/
USER 1001