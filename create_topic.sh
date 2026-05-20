#!/bin/bash

# Create F1 topic
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic f1_events \
  --partitions 2 \
  --replication-factor 1

# Create NASCAR topic
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic nascar_events \
  --partitions 2 \
  --replication-factor 1

# Create LeMans topic
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic lemans_events \
  --partitions 2 \
  --replication-factor 1

# List all topics
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list

# Describe all race topics
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic f1_events

kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic nascar_events

kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic lemans_events