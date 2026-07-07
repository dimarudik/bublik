package dev.bublik.cli.oracle.kafka;

import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.service.StorageService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static dev.bublik.cli.App.getConfigs;
import static dev.bublik.cli.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

public class ToKafkaTest {
    private static final int ROWS = 20000;

    private static final JdbcDatabaseContainer<?> source = new OracleContainer("gvenzl/oracle-free:slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(10))
            .withInitScript("./oracle/kafka/sql/oracle/01_init.sql");

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
    static void setUp() throws Exception {
        source.setPortBindings(Collections.singletonList("1521:1521"));
        source.start();
        target.start();
        createTestTopic(target.getBootstrapServers(), TOPIC_NAME);
    }

    @BeforeEach
    void recreateTopicBeforeEachTest() {
        System.out.println("=== [BeforeEach] Сброс топика '" + TOPIC_NAME + "' перед тестом ===");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, target.getBootstrapServers());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test\";");

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 1. Проверяем, существует ли топик в брокере сейчас
            Set<String> existingTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);

            if (existingTopics.contains(TOPIC_NAME)) {
                // 2. Если топик есть — удаляем его
                adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get(5, TimeUnit.SECONDS);
                System.out.println("Старый топик успешно удален из Kafka.");

                // Минорная пауза (300мс), чтобы брокер в фоне успел очистить файлы логов на диске контейнера
                Thread.sleep(300);
            }

            // 3. Создаем топик заново с 1 партицией
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(5, TimeUnit.SECONDS);
            System.out.println("Топик '" + TOPIC_NAME + "' успешно пересоздан с нуля. Среда готова.");

        } catch (Exception e) {
            System.err.println("Ошибка при пересоздании топика в @BeforeEach: " + e.getMessage());
            throw new RuntimeException("Не удалось подготовить чистый топик перед тестом", e);
        }
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
    }

    @Test
    @DisplayName("Запуск теста сетевой связности с Kafka (SASL_PLAINTEXT / PLAIN)...")
    void testKafkaConnectionAndAuthentication() throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, target.getBootstrapServers());

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test\";");

        props.put("request.timeout.ms", "5000");
        props.put("default.api.timeout.ms", "5000");

        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> topicNames = adminClient.listTopics(new ListTopicsOptions().timeoutMs(3000))
                    .names()
                    .get(5, TimeUnit.SECONDS);
            assertNotNull(topicNames, "Ответ от брокера не должен быть null");
        } catch (Exception e) {
            fail("Сбой теста авторизации", e);
        }
    }

    @Test
    @DisplayName("Запуск сквозного CLI теста миграции данных из Oracle в Kafka")
    void simpleTest() throws Exception {
        Properties sourceProps = getJdbcProperties(source);

        TestResult result = getKafkaResultCount(
                "./oracle/kafka/yaml/ora2kafka.yaml",
                "./oracle/kafka/json/ora2kafka.json",
                ROWS,
                sourceProps,
                target,
                TOPIC_NAME
        );

        assertEquals(result.sourceCount(), result.targetCount());
    }

    @Test
    @DisplayName("Все типы едут из Oracle в Kafka")
    void allTypes() throws Exception {
        Properties sourceProps = getJdbcProperties(source);

        TestResult result = getKafkaResultCount(
                "./oracle/kafka/yaml/ora2kafka.yaml",
                "./oracle/kafka/json/alltypes.json",
                ROWS,
                sourceProps,
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

        cp.getFromProperties().put("url", sourceProperties.getProperty("url"));
        cp.getFromProperties().put("user", sourceProperties.getProperty("user"));
        cp.getFromProperties().put("password", sourceProperties.getProperty("password"));
        cp.getToProperties().put("servers", kafkaContainer.getBootstrapServers());

        StorageService.init(cp, configs, rows, chunkTable, outboxTable);

        long sourceCount = 0;
        for (Config config : configs) {
            String fromQuery = getQuery(config.fromSchemaName() + "." + config.fromTableName(),
                    config.fetchWhereClause() == null ? " 1 = 1 " : config.fetchWhereClause());
            System.out.println("Запрос к источнику: " + fromQuery);
            sourceCount += TestUtils.countRows(sourceProperties, fromQuery);
        }

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
}
