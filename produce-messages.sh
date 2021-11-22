#!/usr/bin/env bash

for i in {0..10000000}; do
  echo "\"$i\": {\"id\": $i,\"uuid\":\"550e8400-e29b-11d4-a716-446655440000\",\"type\":\"FIRST_TYPE\",\"isActive\":true}"
done | docker-compose exec -T schema-registry kafka-avro-console-producer --bootstrap-server kafka:9094 --topic inbox --property parse.key=true --property key.separator=":" --property key.schema='{"type":"string"}' --property value.schema="$(<./src/main/avro/example_payload.avsc)"
