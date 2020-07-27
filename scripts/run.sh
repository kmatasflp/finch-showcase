#!/bin/bash
# This is the master script that acts as the ENTRYPOINT for docker.
#set -x

#Run application
java -cp /app/finch-showcase.jar \
  -Xms2G \
  -Xmx2G \
  -XX:+PrintCommandLineFlags \
  -XX:MaxGCPauseMillis=100 \
  -XX:+UnlockExperimentalVMOptions \
  -XX:+EnableJVMCI \
  -XX:+UseJVMCICompiler \
  com.example.KafkaIngestionServer \
  --outputTopic "$OUTPUT_TOPIC" \
  --sqsQueueUrl "$SQS_QUEUE_URL" \
  --awsRegion "$AWS_REGION" \
  --awsSqsEndpoint "$AWS_SQS_ENPOINT" \
  --metricsEndpointPort "$METRICS_PORT" \
  --metricsEndpointHost "$METRICS_HOST" \
  --containerId "$CONTAINER_ID" \
  --domain "$DOMAIN" \
  --kafkaBootstrapServers "$KAFKA_BOOTSTRAP_SERVER"