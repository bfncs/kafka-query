package us.byteb.kafka.query;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import us.byteb.message.EnumType;
import us.byteb.message.ExamplePayload;

final class AppIT {

  private static final String INBOX_TOPIC = "inbox"; // TODO: configure app
  private static final int PORT = 8080; // TODO: use random port and configure app
  private static final String SCHEMA_REGISTRY_SCOPE = "schema-registry";
  private static final String SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

  private static KafkaContainer kafka;

  @BeforeAll
  static void setup() {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"));
    kafka.start();

    prepareMockSchemaRegistry();

    App.start(kafka.getBootstrapServers(), SCHEMA_REGISTRY_URL);
  }

  @AfterAll
  static void teardown() {
    kafka.close();
    // TODO: gracefully stop app
  }

  @Test
  void check() throws InterruptedException, IOException {
    produceMessage(42);
    // TODO: find an elegant way to guarantee this is not flappy without hardcoded delay here

    final HttpResponse<String> response = request(42);

    assertEquals(200, response.statusCode());
    // TODO: assert expected body
  }

  private static void prepareMockSchemaRegistry() {
    try {
      MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE)
          .register(
              new TopicNameStrategy()
                  .subjectName(INBOX_TOPIC, false, new AvroSchema(ExamplePayload.SCHEMA$)),
              new AvroSchema(ExamplePayload.SCHEMA$));
    } catch (IOException | RestClientException e) {
      throw new IllegalStateException("Unable to prepare mock schema registry client", e);
    }
  }

  private static void produceMessage(final int id) {
    try (final KafkaProducer<String, ExamplePayload> producer =
        new KafkaProducer<>(
            Map.of(
                BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                CLIENT_ID_CONFIG,
                "test-producer-" + UUID.randomUUID(),
                KEY_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class,
                SCHEMA_REGISTRY_URL_CONFIG,
                SCHEMA_REGISTRY_URL))) {

      producer
          .send(
              new ProducerRecord<>(
                  INBOX_TOPIC,
                  String.valueOf(id),
                  new ExamplePayload(id, UUID.randomUUID().toString(), EnumType.FIRST_TYPE, false)))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      throw new IllegalStateException("Unable to produce messages", e);
    }
  }

  private static HttpResponse<String> request(final int id)
      throws IOException, InterruptedException {
    final HttpClient client = HttpClient.newBuilder().build();
    return client.send(
        HttpRequest.newBuilder()
            .GET()
            .uri(URI.create("http://localhost:%d/%d".formatted(PORT, id)))
            .timeout(Duration.ofSeconds(1))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
