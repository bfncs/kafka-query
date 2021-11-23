package us.byteb.kafka.query;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.javalin.http.HttpCode.NOT_FOUND;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.javalin.Javalin;
import io.javalin.http.HttpCode;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class App {

  private static final int HTTP_PORT = 8080;
  private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String KAFKA_TOPIC = "inbox";
  private static final int KAFKA_MAX_POLL_RECORDS = 500;

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    final Map<String, GenericRecord> store = new ConcurrentHashMap<>();
    final AtomicBoolean isInitialized = new AtomicBoolean(false);
    initHttpServer(store, isInitialized);
    runKafkaConsumer(store, isInitialized);
  }

  private static void initHttpServer(
      final Map<String, GenericRecord> store, final AtomicBoolean isInitialized) {
    final Javalin app = Javalin.create().start(HTTP_PORT);
    app.get(
        "/health/ready",
        ctx -> {
          if (isInitialized.get()) {
            ctx.status(HttpCode.OK);
          } else {
            ctx.status(HttpCode.SERVICE_UNAVAILABLE).result("Catching up...");
          }
        });
    app.get(
        "/{key}",
        ctx -> {
          ctx.header("X-STORE-SIZE", String.valueOf(store.size()));
          ctx.header("X-READY", isInitialized.toString());

          final GenericRecord result = store.get(ctx.pathParam("key"));
          if (result == null) {
            ctx.status(NOT_FOUND);
          } else {
            ctx.result(result.toString());
          }
        });
  }

  private static void runKafkaConsumer(
      final Map<String, GenericRecord> store, final AtomicBoolean isInitialized) {
    final String consumerGroupId = "kafka-query-" + UUID.randomUUID();
    LOGGER.info("Starting consumer with group id {}", consumerGroupId);

    final Instant initializationStart = Instant.now();
    BigInteger messagesReceived = BigInteger.ZERO;

    try (final KafkaConsumer<String, GenericRecord> consumer =
        new KafkaConsumer<>(buildKafkaConsumerConfig(consumerGroupId))) {
      consumer.subscribe(List.of(KAFKA_TOPIC));

      Executors.newSingleThreadScheduledExecutor()
          .scheduleAtFixedRate(
              () -> {
                final Map<MetricName, ? extends Metric> metrics = consumer.metrics();

                // This updates very slowly and „NaN“ means there is no lag 🤔

                final String lags =
                    metrics.entrySet().stream()
                        .filter(m -> m.getKey().name().equals("records-lag-max"))
                        .map(m -> m.getValue().metricName() + ": " + m.getValue().metricValue())
                        .collect(Collectors.joining("\n"));
                System.out.println(lags);
              },
              0,
              2,
              TimeUnit.SECONDS);

      while (true) {
        final ConsumerRecords<String, GenericRecord> records =
            consumer.poll(Duration.ofMillis(100));
        messagesReceived = messagesReceived.add(BigInteger.valueOf(records.count()));

        if (!isInitialized.get() && store.size() > 0 && records.count() < KAFKA_MAX_POLL_RECORDS) {
          LOGGER.info(
              "Caught up after receiving {} messages in {}ms. Currently {} items in store.",
              messagesReceived,
              Duration.between(initializationStart, Instant.now()).toMillis(),
              store.size());
          isInitialized.set(true);
        }

        for (final ConsumerRecord<String, GenericRecord> record : records) {
          LOGGER.debug("key = {}, value = {}", record.key(), record.value());
          if (record.value() == null) {
            store.remove(record.key());
          } else {
            store.put(record.key(), record.value());
          }
        }
      }
    }
  }

  private static Properties buildKafkaConsumerConfig(final String consumerGroupId) {
    final Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
    props.put(GROUP_ID_CONFIG, consumerGroupId);
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(MAX_POLL_RECORDS_CONFIG, KAFKA_MAX_POLL_RECORDS);
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

    return props;
  }
}
