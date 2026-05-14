FROM confluentinc/cp-kafka-connect:7.5.0

# Install a Java 11 compatible Debezium connector version.
USER root
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:2.4.2-1
USER appuser