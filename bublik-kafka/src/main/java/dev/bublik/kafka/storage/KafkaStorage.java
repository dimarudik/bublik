package dev.bublik.kafka.storage;

import dev.bublik.core.exception.SourceSQLException;
import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.kafka.model.FieldRuntimeContext;
import dev.bublik.kafka.service.AvroTypeMapper;
import dev.bublik.kafka.service.TypeMapperFactory;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaStorage extends Storage {
    private static final Logger log = LoggerFactory.getLogger(KafkaStorage.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final String topic;
    private final Map<String, AvroRowProducer> producerCache = new ConcurrentHashMap<>();

    public KafkaStorage(KafkaProducer<String, byte[]> kafkaProducer, String topic) {
        super(new ConnectionProperty());
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    protected KafkaStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        Properties kafkaProperties = buildKafkaProperties(connectionProperty.getToProperties());
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties);
        this.topic = connectionProperty.getToProperties().get("topic");
    }

    public KafkaStorage(StorageClass storageClass, ConnectionProperty connectionProperty, Table outboxTable) {
        this(storageClass, connectionProperty);
    }

    @Override
    public <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        try {
            ResultSet rs = (ResultSet) chunk.getResultSet();
            List<Column2Column> c2c = chunk.getT2t().column2Columns();

            AvroRowProducer currentTableProducer = producerCache.computeIfAbsent(tableName, tName -> {
                Schema dynamicSchema = buildAvroSchemaFromC2C(c2c, chunk.getConfig());
                return new AvroRowProducer(this.kafkaProducer, dynamicSchema, c2c);
            });

            return currentTableProducer.streamResultSetToKafka(rs, topic);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Программная высокопроизводительная сборка Avro схемы из List<Column2Column>
     * Гарантирует Nullable-статус (UNION) для всех полей, защищая от NullPointerException.
     */
    private Schema buildAvroSchemaFromC2C(List<Column2Column> c2c, Config config) {
        Map<String, Object> avroSchemaMap = config.avroSchema();

        String recordName = avroSchemaMap != null && avroSchemaMap.containsKey("name")
                ? String.valueOf(avroSchemaMap.get("name"))
                : "DynamicRecord";

        String namespace = avroSchemaMap != null && avroSchemaMap.containsKey("namespace")
                ? String.valueOf(avroSchemaMap.get("namespace"))
                : "dev.bublik.dynamic";

        SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = SchemaBuilder
                .record(recordName)
                .namespace(namespace)
                .fields();

        for (Column2Column link : c2c) {
            String avroFieldName = link.targetColumn().columnName();
            String avroType = link.targetColumn().columnType().toLowerCase();

            // Создаем базовую схему типа
            Schema fieldSchema = switch (avroType) {
                case "int" -> Schema.create(Schema.Type.INT);
                case "long" -> Schema.create(Schema.Type.LONG);
                case "double" -> Schema.create(Schema.Type.DOUBLE);
                case "float" -> Schema.create(Schema.Type.FLOAT);
                case "boolean" -> Schema.create(Schema.Type.BOOLEAN);
                case "bytes" -> Schema.create(Schema.Type.BYTES);
                default -> Schema.create(Schema.Type.STRING);
            };

            // ЖЕЛЕЗОБЕТОННАЯ ЗАЩИТА: Каждое поле принудительно делаем союзом [NULL, ТИП].
            // Это полностью соответствует вашему JSON-файлу маппинга таблиц.
            Schema nullSchema = Schema.create(Schema.Type.NULL);
            Schema unionSchema = Schema.createUnion(java.util.List.of(nullSchema, fieldSchema));

            // Добавляем поле в сборщик с обязательным дефолтом null
            fieldsAssembler = fieldsAssembler.name(avroFieldName)
                    .type(unionSchema)
                    .withDefault(null);
        }

        return fieldsAssembler.endRecord();
    }

    private Properties buildKafkaProperties(Map<String, String> yamlProps) {
        Properties props = new Properties();

        // 1. Проверяем обязательный параметр серверов
        String servers = yamlProps.get("servers");
        if (servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("Параметр 'servers' (адрес брокеров Kafka) обязателен в конфигурации!");
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        // Стандартные сериализаторы всегда заполняем жестко
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // 2. Безопасное извлечение и форматирование JAAS
        String user = yamlProps.get("user");
        String password = yamlProps.get("password");
        String jaasFormatter = yamlProps.get("jaasCfg.formatter");

        if (user != null && password != null && jaasFormatter != null) {
            String finalJaasConfig = String.format(jaasFormatter, user, password);
            props.put("sasl.jaas.config", finalJaasConfig);
        }

        // 3. БЕЗОПАСНОЕ КОПИРОВАНИЕ ОПЦИОНАЛЬНЫХ ПАРАМЕТРОВ (Защита от NullPointerException)
        // Используем приватный хелпер putIfNotNull для проверки каждой строки
        putIfNotNull(props, "security.protocol", yamlProps.get("security.protocol"));
        putIfNotNull(props, "sasl.mechanism", yamlProps.get("sasl.mechanism"));
        putIfNotNull(props, "ssl.truststore.location", yamlProps.get("ssl.truststore.location"));
        putIfNotNull(props, "ssl.truststore.password", yamlProps.get("ssl.truststore.password"));

        // 4. Системные оптимизации для многопоточного стриминга (threadCount: 4)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return props;
    }

    private void putIfNotNull(Properties props, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            props.put(key, value);
        }
    }

    @Override
    public void close() throws Exception {
        kafkaProducer.close();
    }

    private static class AvroRowProducer {
        private final KafkaProducer<String, byte[]> producer;
        private final Schema schema;
        private final List<FieldRuntimeContext> cachedFieldsContext;
        private final String firstFieldName;

        public AvroRowProducer(KafkaProducer<String, byte[]> producer, Schema schema, List<Column2Column> c2c) {
            this.producer = producer;
            this.schema = schema;

            // КРИТИЧЕСКАЯ ОПТИМИЗАЦИЯ ПАМЯТИ: Сборка и компиляция контекста полей
            // Происходит СТРОГО один раз при создании продюсера для таблицы, а не на миллионах строк!
            this.cachedFieldsContext = c2c.stream().map(link -> {
                String dbLookupName = (link.sourceExpression() != null)
                        ? link.targetColumn().columnName()
                        : link.sourceColumn().columnName();

                String avroFieldName = link.targetColumn().columnName();
                String avroType = link.targetColumn().columnType();

                // Находим нужный синглтон-маппер для этого поля один раз
                AvroTypeMapper mapper = TypeMapperFactory.getMapper(avroType);

                return new FieldRuntimeContext(avroFieldName, dbLookupName, mapper);
            }).toList();

            // Кешируем имя первого поля для генерации messageKey
            this.firstFieldName = c2c.get(0).targetColumn().columnName();
        }

        public LogMessage streamResultSetToKafka(ResultSet rs, String topicName) throws SQLException, IOException {
            int rowCount = 0;

            // Буферы изолированы в стеке вызывающего потока (Thread-safe)
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            GenericRecord record = new GenericData.Record(schema);
            BinaryEncoder encoder = null;

            while (rs.next()) {

                // Полиморфизм в действии: итерируемся по готовому кешу контекста полей
                for (FieldRuntimeContext context : cachedFieldsContext) {

                    // Вызов стратегии маппинга без спагетти-кода и switch-case
                    Object value = context.mapper().getValue(rs, context.dbLookupName());

                    // Записываем значение в Avro-record по имени поля
                    record.put(context.avroFieldName(), value);
                }

                // --- БИНАРНАЯ СЕРИАЛИЗАЦИЯ И ОТПРАВКА ---
                out.reset();

                encoder = EncoderFactory.get().binaryEncoder(out, encoder);
                writer.write(record, encoder);
                encoder.flush();

                byte[] rowBytes = out.toByteArray();

                // Извлекаем ключ сообщения из кешированного имени первого поля
                Object keyObj = record.get(firstFieldName);
                String messageKey = (keyObj != null) ? keyObj.toString() : String.valueOf(rowCount);

                // Асинхронно бросаем в сетевой буфер Kafka
                producer.send(new ProducerRecord<>(topicName, messageKey, rowBytes));
                rowCount++;
            }

            producer.flush();
            System.out.println("Поток [" + Thread.currentThread().getName() + "] успешно завершил чанк. Отправлено строк: " + rowCount);
            return new LogMessage(0, System.currentTimeMillis(), " To Kafka");
        }
    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException {

    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows, boolean sync) throws SQLException {

    }

    @Override
    public void createGlobalOutbox() throws SQLException {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, V, W> void insertColumnValue(List<ColumnValue<V>> columnValues, Chunk<K, T, S, R> chunk, W writer) throws SQLException {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException, SourceSQLException, IOException {
        return null;
    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> void closeWriter(W writer, Chunk<K, T, S, R> chunk, String tableName) throws SQLException {

    }

    @Override
    public void insertProcessedChunkInfo(Chunk<?, ?, ?, ?> chunk) throws SQLException {

    }

    @Override
    public boolean isChunkProcessed(Chunk<?, ?, ?, ?> chunk) throws SQLException {
        return false;
    }

    @Override
    public void dropOutboxTable(boolean sync) throws SQLException {

    }

    @Override
    public List<Config> copyConfigs(List<Config> cfgs) {
        return List.of();
    }

    @Override
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<Config> configs, Storage targetStorage) throws SQLException {
        return List.of();
    }

    @Override
    public String buildStartEndOfChunk(Config config, Table sourceTable) {
        return "";
    }

    @Override
    public void closeStorage() {

    }

    @Override
    public String buildFetchStatement(Config config, Table2Table t2t) {
        return "";
    }

    @Override
    public Map<String, Column> readTargetColumnsAndTypes(Connection connectionTo, Chunk<?, ?, ?, ?> chunk) {
        return Map.of();
    }

    @Override
    public Map<Table, Table> configsToTables(List<Config> configs, Storage targetStorage) {
        return Map.of();
    }

    @Override
    public Table configToTable(String schemaName, String tableName) {
        return new PseudoTable(schemaName, tableName);
    }

    @Override
    public Table getTargetTableBySourceTable(Table table) {
        return null;
    }

    @Override
    public Table getSourceTableByTargetTable(Table table) {
        return null;
    }

    @Override
    public <S extends AutoCloseable> S getPoolConnection() throws SQLException {
        return null;
    }

    @Override
    public <S extends AutoCloseable> S getSession() {
        return null;
    }

    @Override
    public String getStorageVersion() {
        return "";
    }

    @Override
    public int getStorageMajorVersion() {
        return 0;
    }

    @Override
    public <S extends AutoCloseable> void setSession(S session) {

    }

    @Override
    public void enrichTable(Table sourceTable) throws SQLException {

    }

    @Override
    public void enrichTable(Table sourceTable, Table targetTable) throws SQLException {

    }

    @Override
    public List<Column2Column> getColumn2Column(Table sourceTable, Table targetTable, Config config) {
        return List.of();
    }

    @Override
    public Table2Table getTable2Table(Table sourceTable, Table targetTable, List<Column2Column> c2c, Config config) {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
