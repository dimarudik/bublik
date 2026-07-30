package dev.bublik.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.bublik.cassandra.storage.CassandraStorage;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.PseudoTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.storage.Storage;
import dev.bublik.kafka.storage.KafkaStorage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaMigrationTest {
    int batchSize = 256;
    static final CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra"));

    static final ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.2")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("ssl/kafka_plain_jaas.conf"),
                    "/etc/kafka/secrets/kafka_plain_jaas.conf"
            )
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_plain_jaas.conf");

    static CqlSession cassandraSession;
    static final String TOPIC_NAME = "cassandra-users-topic";
    static final String KEYSPACE = "bublik_keyspace";
    static final String TABLE_NAME = "source_users";

    @BeforeAll
    static void beforeAll() throws Exception {
        cassandra.start();
        kafkaContainer.start();

        cassandraSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
                .withLocalDatacenter("datacenter1")
                .build();

        cassandraSession.execute("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        cassandraSession.execute("CREATE TABLE " + KEYSPACE + "." + TABLE_NAME + " (id int PRIMARY KEY, user_name text);");
        cassandraSession.execute("INSERT INTO " + KEYSPACE + "." + TABLE_NAME + " (id, user_name) VALUES (1, 'Alice');");
        cassandraSession.execute("INSERT INTO " + KEYSPACE + "." + TABLE_NAME + " (id, user_name) VALUES (2, 'Bob');");
        cassandraSession.execute("INSERT INTO " + KEYSPACE + "." + TABLE_NAME + " (id, user_name) VALUES (3, 'Charlie');");
    }

    @AfterAll
    static void afterAll() {
        if (cassandraSession != null) cassandraSession.close();
        cassandra.stop();
        kafkaContainer.stop();
    }

    @Test
    @DisplayName("Миграция из Cassandra в Kafka через явный конструктор")
    void testCassandraToKafkaMigration() throws Exception {
        Table sourceChunkTable = new PseudoTable(KEYSPACE, "bublik");
        Storage sourceStorage = new CassandraStorage.Builder()
                .cqlSession(cassandraSession)
                .batchSize(batchSize)
                .outboxTable(sourceChunkTable)
                .build();

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.mechanism", "PLAIN");
        kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test\";");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);

        Storage targetStorage = new KafkaStorage.Builder()
                .kafkaProducer(producer)
                .topic(TOPIC_NAME)
                .build();


        List<Config> configs = new ArrayList<>();

        Map<String, String> columnToColumn = new LinkedHashMap<>();
        columnToColumn.put("id", "id");
        columnToColumn.put("user_name", "user_name");

        Map<String, Object> avroSchema = new LinkedHashMap<>();
        avroSchema.put("type", "record");
        avroSchema.put("name", "UserRecord");
        avroSchema.put("namespace", "dev.bublik");

        List<Map<String, Object>> fields = new ArrayList<>();

        Map<String, Object> idField = new LinkedHashMap<>();
        idField.put("name", "id");
        idField.put("type", List.of("null", "int"));
        idField.put("default", null);

        Map<String, Object> nameField = new LinkedHashMap<>();
        nameField.put("name", "user_name");
        nameField.put("type", List.of("null", "string"));
        nameField.put("default", null);

        fields.add(idField);
        fields.add(nameField);
        avroSchema.put("fields", fields);

        Config config = Config.builder()
                .from(KEYSPACE, TABLE_NAME)
                .to(TOPIC_NAME)
                .columnToColumn(columnToColumn)
                .avroSchema(avroSchema)
                .build();
        configs.add(config);

        sourceStorage.start(targetStorage, configs, 1000);

        long kafkaMessageCount = countMessagesInKafka(kafkaContainer.getBootstrapServers(), TOPIC_NAME);
        assertEquals(3, kafkaMessageCount, "В топик Kafka должно быть успешно отправлено 3 Avro-записи из Cassandra!");

        sourceStorage.closeStorage();
        targetStorage.closeStorage();
    }

    private static long countMessagesInKafka(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cassandra-verification-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test\";");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        long count = 0;
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    count += records.count();
                }
            }
        }
        return count;
    }
}
