#!/bin/bash
# Run this once after Kafka is healthy to create the topics

KAFKA_CONTAINER=stockforge-kafka
BOOTSTRAP=localhost:9092

echo "Creating Kafka topics..."

docker exec $KAFKA_CONTAINER kafka-topics \
  --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic stock_prices \
  --partitions 3 \
  --replication-factor 1

docker exec $KAFKA_CONTAINER kafka-topics \
  --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic portfolio_transactions \
  --partitions 3 \
  --replication-factor 1

echo "Topics created:"
docker exec $KAFKA_CONTAINER kafka-topics \
  --list --bootstrap-server $BOOTSTRAP
