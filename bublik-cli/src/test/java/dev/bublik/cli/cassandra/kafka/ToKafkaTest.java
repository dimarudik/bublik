package dev.bublik.cli.cassandra.kafka;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import dev.bublik.cassandra.storage.CSPool;
import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.DummyTable;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ToKafkaTest {
    private final static int ROWS = 20000;

    private static CassandraContainer source = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_USER_DEFINED_FUNCTIONS_ENABLED", "true")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withCopyToContainer(MountableFile.forClasspathResource("./cassandra/cassandra/conf/docker-entrypoint.sh"), "/usr/local/bin/docker-entrypoint.sh")
            .withInitScript("./cassandra/kafka/sql/cs-init.cql")
            .withExposedPorts(9042);

    private static final ConfluentKafkaContainer target = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.2")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("ssl/kafka_plain_jaas.conf"),
                    "/etc/kafka/secrets/kafka_plain_jaas.conf"
            )
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_plain_jaas.conf");


    private static final String TOPIC_NAME = "test";

    @BeforeAll
    static void setUp() throws SQLException, IOException, InterruptedException {
        source.setPortBindings(Collections.singletonList("9042:9042"));
        source.start();
        target.start();
        createTestTopic(target.getBootstrapServers(), TOPIC_NAME);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        source.stop();
        target.stop();
    }

    @Test
    @DisplayName("Запуск сквозного CLI теста миграции данных из Cassandra в Kafka")
    void simpleTest() throws Exception {
        Properties sourceProperties = getPropertiesOfCassandra("localhost:9042");
        initSourceData(sourceProperties, 70_030, "test.t4");
        initSourceData(sourceProperties, 71_130, "test.t5");
        TestResult result = getKafkaResultCount(
                "./cassandra/kafka/yaml/cs2kafka.yaml",
                "./cassandra/kafka/json/cs2kafka.json",
                ROWS,
                sourceProperties,
                target,
                TOPIC_NAME
        );
        assertEquals(result.sourceCount(), result.targetCount());
    }

    public TestResult getKafkaResultCount(String connectionPropertyFile,
                                          String mappingFile,
                                          int rows,
                                          Properties sourceProperties,
                                          ConfluentKafkaContainer kafkaContainer,
                                          String targetTopic) throws Exception {

        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));
        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));

        cp.getToProperties().put("servers", kafkaContainer.getBootstrapServers());
        Table chunkTable = new DummyTable(cp.getToProperty().getProperty("keyspace"), "chunk");

        StorageService.init(cp, configs, rows, chunkTable);

        long sourceCount = 141160;
        long targetCount = countMessagesInKafka(kafkaContainer.getBootstrapServers(), targetTopic);

        return new TestResult(sourceCount, targetCount);
    }

    private static long countMessagesInKafka(String bootstrapServers, String topic) {
        System.out.println("Consumer запускает подсчет сообщений в топике '" + topic + "'...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-verification-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test\";");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        long messageCount = 0;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    messageCount += records.count();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Не удалось вычитать данные из Kafka для проверки теста", e);
        }

        System.out.println("Проверка завершена. Consumer обнаружил в топике записей: " + messageCount);
        return messageCount;
    }

    private static void createTestTopic(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test\";");

        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(5, TimeUnit.SECONDS);
            System.out.println("Тестовый топик '" + topic + "' успешно создан в контейнере Kafka 8.2.2.");
        } catch (Exception e) {
            System.err.println("Не удалось создать топик Kafka: " + e.getMessage());
            throw new RuntimeException("Ошибка инициализации тестового окружения Kafka", e);
        }
    }

    private void initSourceData(Properties properties, int rows, String tableName) {
        CSPool csPool = new CSPool(properties, 2);
        CqlSession cqlSession = csPool.getCqlSession();
        String truncateTable = "truncate &{tableName}".replace("&{tableName}", tableName);
        cqlSession.execute(truncateTable);
        String insertQuery =
                "insert into &{tableName} (id, uid, v1, v2, v3, v4) values (:id, :uid, :v1, :v2, :v3, :v4)"
                        .replace("&{tableName}", tableName);
        PreparedStatement preparedStatement = cqlSession.prepare(insertQuery);
        BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
        for (int i = 0; i < rows; i++) {
            Object[] values = new Object[]{i % 16, i, i + 10, i + 100, i + 1000, "v4" + i};
            BatchableStatement<?> statement = preparedStatement.bind(values);
            batchStatementBuilder.addStatement(statement);
            if (i % Integer.parseInt(properties.getProperty("batchSize")) == 0) {
                cqlSession.execute(batchStatementBuilder.build());
                batchStatementBuilder.clearStatements();
            }
        }
        cqlSession.execute(batchStatementBuilder.build());
        batchStatementBuilder.clearStatements();
        csPool.closeCqlSession();
    }

    private Properties getPropertiesOfCassandra(String hosts) {
        Properties properties = new Properties();
        properties.setProperty("class", "org.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", hosts);
        properties.setProperty("user", "cassandra");
        properties.setProperty("password", "cassandra");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }
}
