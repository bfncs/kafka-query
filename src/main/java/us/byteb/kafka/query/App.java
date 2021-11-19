package us.byteb.kafka.query;

import static io.javalin.http.HttpCode.NOT_FOUND;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import io.javalin.Javalin;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.javalin.http.HttpCode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class App {

  private static final int HTTP_PORT = 8080;
  private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String KAFKA_TOPIC = "inbox";
  private static final int KAFKA_MAX_POLL_RECORDS = 500;

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    final Map<String, String> store = new ConcurrentHashMap<>();
    final AtomicBoolean isInitialized = new AtomicBoolean(false);
    initHttpServer(store, isInitialized);
    runKafkaConsumer(store, isInitialized);
  }

  private static void initHttpServer(final Map<String, String> store, final AtomicBoolean isInitialized) {
    final Javalin app = Javalin.create().start(HTTP_PORT);
    app.get("/health/ready", ctx -> {
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

          final String result = store.get(ctx.pathParam("key"));
          if (result == null) {
            ctx.status(NOT_FOUND);
          } else {
            ctx.result(result);
          }
        });
  }

  private static void runKafkaConsumer(final Map<String, String> store, final AtomicBoolean isInitialized) {
    final String consumerGroupId = "kafka-query-" + UUID.randomUUID();
    LOGGER.info("Starting consumer with group id {}", consumerGroupId);

    final Instant initializationStart = Instant.now();
    BigInteger messagesReceived = BigInteger.ZERO;

    try (final KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(buildKafkaConsumerConfig(consumerGroupId))) {
      consumer.subscribe(List.of(KAFKA_TOPIC));

      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        messagesReceived = messagesReceived.add(BigInteger.valueOf(records.count()));

        if (!isInitialized.get() && store.size() > 0 && records.count() < KAFKA_MAX_POLL_RECORDS) {
          LOGGER.info("Caught up after receiving {} messages in {}ms. Currently {} items in store.", messagesReceived, Duration.between(initializationStart, Instant.now()).toMillis(), store.size());
          isInitialized.set(true);
        }

        for (final ConsumerRecord<String, String> record : records) {
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
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return props;
  }
}
