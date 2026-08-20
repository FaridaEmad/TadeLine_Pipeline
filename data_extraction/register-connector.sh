#!/bin/bash
set -e

KAFKA_CONNECT_URL="http://kafka-connect:8083"
CONNECTOR_NAME="earthquakes-postgres-json-connector"

echo "Waiting for Kafka Connect to be ready..."
until curl --silent --fail "${KAFKA_CONNECT_URL}/connectors" > /dev/null; do
  sleep 3
done

# checking idempotecy -> skip if it already exists, register if it doesn't
if curl --silent --fail "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}" > /dev/null 2>&1; then
  echo "Connector '${CONNECTOR_NAME}' already exists — skipping registration."
else
  echo "Registering connector '${CONNECTOR_NAME}'..."
  curl -X POST "${KAFKA_CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "earthquakes-postgres-json-connector",
      "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "maritime_logistics",
        "database.server.name": "earthquakes-json-db",
        "table.include.list": "public.earthquakes",
        "plugin.name": "pgoutput",
        "slot.name": "earthquakes_json_slot",
        "publication.name": "earthquakes_json_publication",
        "topic.prefix": "earthquakes_json",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.add.fields": "op,ts_ms",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "double",
        "time.precision.mode": "connect"
      }
    }'
  echo ""
  echo "Connector registered."
fi






