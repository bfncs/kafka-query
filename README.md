# Kafka Query Playground

Query kafka topic contents by message key.

## Usage

1. Build application: `mvn clean verify`
2. Start Kafka & Zookeeper: `docker compose up -d`
3. Produce messages in compacted topic: `./produce-messages.sh`
4. Start application: `java -jar target/kafka-query-1.0-SNAPSHOT-jar-with-dependencies.jar`
5. Query kafka topic contents by message key: `curl localhost:8080/<KEY>`

##      Tools

```sh
# Consume compacted topic
docker-compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:9094 --topic inbox
```