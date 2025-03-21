#!/bin/bash

# Define variables
KAFKA_IMAGE="kafka.sif"
INSTANCE_NAME="kafka-instance"
KAFKA_CONFIG_HOST="kafka-config/server.properties"  # Local editable copy
KAFKA_CONFIG_CONTAINER="/opt/kafka/config/server.properties"
ZOOKEEPER_CONFIG="/opt/kafka/config/zookeeper.properties"
KAFKA_SERVER="/opt/kafka/bin/kafka-server-start.sh"
ZOOKEEPER_SERVER="/opt/kafka/bin/zookeeper-server-start.sh"

HOST_IP=$(<base_node_ip.txt)
echo "IP:${HOST_IP}"



# Ensure Kafka data/logs directories exist on host
rm -fr $HOME/kafka-logs/*
rm -fr $HOME/kafka-data/*
rm -rf /tmp/kafka-logs/*  # Some Kafka setups store logs in /tmp/
rm -rf /tmp/zookeeper/*  # Clean up Zookeeper state
mkdir -p $HOME/kafka-logs
mkdir -p $HOME/kafka-data


# # Modify Kafka configuration inside the container for external access
# echo "Configuring Kafka for Zookeeper mode..."
# apptainer exec $KAFKA_IMAGE bash -c "

# Prep Kafka config override
mkdir -p kafka-config


echo "zookeeper.connect=localhost:2181" >> "$KAFKA_CONFIG_HOST"
echo "listeners=PLAINTEXT://0.0.0.0:9092" >> "$KAFKA_CONFIG_HOST"
echo "advertised.listeners=PLAINTEXT://${HOST_IP}:9092" >> "$KAFKA_CONFIG_HOST"
echo "log.dirs=/opt/kafka/logs" >> "$KAFKA_CONFIG_HOST"
echo "offsets.topic.replication.factor=1" >> "$KAFKA_CONFIG_HOST"

# Start Apptainer instance
echo "Starting Apptainer instance: $INSTANCE_NAME..."
apptainer instance start \
          --writable-tmpfs \
          --bind "$KAFKA_CONFIG_HOST:$KAFKA_CONFIG_CONTAINER" \
          --bind $HOME/kafka-data:/data \
          --bind $HOME/kafka-logs:/opt/kafka/logs $KAFKA_IMAGE $INSTANCE_NAME
# apptainer instance start --writable-tmpfs  $KAFKA_IMAGE $INSTANCE_NAME

# Start Zookeeper inside the instance
echo "Starting Zookeeper..."
apptainer exec instance://$INSTANCE_NAME $ZOOKEEPER_SERVER $ZOOKEEPER_CONFIG &

# Wait for Zookeeper to be fully up before starting Kafka
echo "Waiting for Zookeeper to start..."
sleep 10

# echo "Cleaning up Zookeeper state before Kafka startup..."
# apptainer exec instance://$INSTANCE_NAME /opt/kafka/bin/zookeeper-shell.sh localhost:2181 <<EOF
# deleteall /brokers/ids/0
# quit
# EOF
# sleep 10

# Start Kafka broker inside the instance
echo "Starting Kafka broker..."
apptainer exec instance://$INSTANCE_NAME $KAFKA_SERVER $KAFKA_CONFIG_CONTAINER &

echo "Kafka is now running inside Apptainer instance: $INSTANCE_NAME"


# Wait for Kafka to be fully up before creating topics
echo "Waiting for Kafka to stabilize..."
sleep 10

# Create a Kafka topic called "interception"
echo "Creating Kafka topic: interception..."
apptainer exec instance://$INSTANCE_NAME /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic interception

echo "Kafka is now running and the topic 'interception' has been created."

touch flag.txt