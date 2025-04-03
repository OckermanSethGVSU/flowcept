#!/bin/bash

# Define variables
KAFKA_IMAGE="kafka.sif"
INSTANCE_NAME="kafka-instance"
KAFKA_CONFIG_HOST="kafka-config/server.properties"  # Local editable copy
KAFKA_CONFIG_CONTAINER="/opt/kafka/config/server.properties"
KAFKA_SERVER="/opt/kafka/bin/kafka-server-start.sh"

HOST_IP=$(<base_node_ip.txt)
# HOST_IP="localhost"
echo "IP:${HOST_IP}"

dir="/tmp"
# Clean up Kafka state directories
rm -fr $dir/kafka-logs/*
rm -fr $dir/kafka-data/*
rm -rf /tmp/kafka-logs/*

mkdir -p $dir/kafka-logs
mkdir -p $dir/kafka-data
chmod -R g+w $dir/

mkdir -p kafka-config
chmod -R g+w kafka-config/

# Generate unique broker ID if needed
BROKER_ID=1
CLUSTER_ID=$(uuidgen)  # Generate a random cluster ID

# Update Kafka configuration for KRaft mode
cat <<EOF > "$KAFKA_CONFIG_HOST"
process.roles=broker,controller
node.id=${BROKER_ID}
controller.quorum.voters=${BROKER_ID}@${HOST_IP}:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://${HOST_IP}:9092,CONTROLLER://${HOST_IP}:9093
advertised.listeners=PLAINTEXT://${HOST_IP}:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/opt/kafka/logs
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF

chmod g+w $KAFKA_CONFIG_HOST

# Start Apptainer instance
echo "Starting Apptainer instance: $INSTANCE_NAME..."
apptainer instance start \
  --writable-tmpfs \
  --bind "$KAFKA_CONFIG_HOST:$KAFKA_CONFIG_CONTAINER" \
  --bind $dir/kafka-data:/data \
  --bind $dir/kafka-logs:/opt/kafka/logs $KAFKA_IMAGE $INSTANCE_NAME

# Format the Kafka logs directory with the new cluster ID
echo "Formatting log dirs for KRaft mode..."
apptainer exec instance://$INSTANCE_NAME /opt/kafka/bin/kafka-storage.sh format \
  -t $CLUSTER_ID \
  -c $KAFKA_CONFIG_CONTAINER

# Start Kafka in KRaft mode
echo "Starting Kafka (KRaft mode)..."
apptainer exec instance://$INSTANCE_NAME $KAFKA_SERVER $KAFKA_CONFIG_CONTAINER &

echo "Kafka is now running in KRaft mode inside Apptainer instance: $INSTANCE_NAME"

# Wait for Kafka to be fully up before creating topics
echo "Waiting for Kafka to stabilize..."
sleep 10

# Create a Kafka topic called "interception"
echo "Creating Kafka topic: interception..."
apptainer exec instance://$INSTANCE_NAME /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server ${HOST_IP}:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic interception

echo "Kafka is now running and the topic 'interception' has been created."

touch flag.txt
