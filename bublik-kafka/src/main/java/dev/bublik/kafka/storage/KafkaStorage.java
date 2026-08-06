package dev.bublik.kafka.storage;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.JDBCStorage;
import dev.bublik.core.storage.Storage;
import dev.bublik.core.storage.StorageClass;
import dev.bublik.kafka.model.FieldRuntimeContext;
import dev.bublik.kafka.service.AvroTypeMapper;
import dev.bublik.kafka.service.ObjectTypeMapper;
import dev.bublik.kafka.service.ObjectTypeMapperFactory;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaStorage extends Storage {
    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final String topic;
    private final Map<String, AvroRowProducer> producerCache = new ConcurrentHashMap<>();
    private final Map<String, ObjectRowProducer> valueCache = new ConcurrentHashMap<>();

    protected KafkaStorage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        super(storageClass, connectionProperty);
        Properties kafkaProperties = buildKafkaProperties(connectionProperty.getToProperties());
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties);
        this.topic = connectionProperty.getToProperties().get("topic");
    }

    public KafkaStorage(StorageClass storageClass, ConnectionProperty connectionProperty, Table outboxTable) {
        this(storageClass, connectionProperty);
    }

    private KafkaStorage(Builder builder) {
        super(builder);
        this.kafkaProducer = builder.kafkaProducer;
        this.topic = builder.topic;
    }

    public static class Builder extends Storage.Builder<KafkaStorage, Builder> {
        private final KafkaProducer<String, byte[]> kafkaProducer;
        private final String topic;

        public Builder(KafkaProducer<String, byte[]> kafkaProducer, String topic) {
            this.kafkaProducer = kafkaProducer;
            this.topic = topic;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public KafkaStorage build() {
            validate();
            return new KafkaStorage(this);
        }
    }


    @Override
    public <K, T, S extends AutoCloseable, R> LogMessage transfer(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        try {
            List<Column2Column> c2c = chunk.getT2t().column2Columns();
            if (chunk.getSourceStorage() instanceof JDBCStorage) {

                String schemaName = chunk.getConfig().fromSchemaName();
                String globalCacheKey = (schemaName != null && !schemaName.isBlank())
                        ? schemaName + "." + tableName
                        : tableName;

                AvroRowProducer currentTableProducer = producerCache.get(globalCacheKey);

                if (currentTableProducer == null) {
                    Schema dynamicSchema = buildAvroSchemaFromC2C(c2c, chunk.getConfig());
                    AvroRowProducer newProducer = new AvroRowProducer(this.kafkaProducer, dynamicSchema, c2c);

                    AvroRowProducer existing = producerCache.putIfAbsent(globalCacheKey, newProducer);
                    currentTableProducer = (existing != null) ? existing : newProducer;
                }

                return currentTableProducer.streamResultSetToKafka(chunk, topic);
            } else {
                return chunk.getSourceStorage().transfer(chunk, tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("KafkaStorage transfer pipeline failed", e);
        }
    }

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

            Schema fieldSchema = switch (avroType) {
                case "int" -> Schema.create(Schema.Type.INT);
                case "long" -> Schema.create(Schema.Type.LONG);
                case "double" -> Schema.create(Schema.Type.DOUBLE);
                case "float" -> Schema.create(Schema.Type.FLOAT);
                case "boolean" -> Schema.create(Schema.Type.BOOLEAN);
                case "bytes" -> Schema.create(Schema.Type.BYTES);
                default -> Schema.create(Schema.Type.STRING);
            };

            Schema nullSchema = Schema.create(Schema.Type.NULL);
            Schema unionSchema = Schema.createUnion(java.util.List.of(nullSchema, fieldSchema));

            fieldsAssembler = fieldsAssembler.name(avroFieldName)
                    .type(unionSchema)
                    .withDefault(null);
        }

        return fieldsAssembler.endRecord();
    }

    private Properties buildKafkaProperties(Map<String, String> yamlProps) {
        Properties props = new Properties();

        String servers = yamlProps.get("servers");
        if (servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("Параметр 'servers' (адрес брокеров Kafka) обязателен в конфигурации!");
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        String user = yamlProps.get("user");
        String password = yamlProps.get("password");
        String jaasFormatter = yamlProps.get("jaasCfg.formatter");

        if (user != null && password != null && jaasFormatter != null) {
            String finalJaasConfig = String.format(jaasFormatter, user, password);
            props.put("sasl.jaas.config", finalJaasConfig);
        }

        putIfNotNull(props, "security.protocol", yamlProps.get("security.protocol"));
        putIfNotNull(props, "sasl.mechanism", yamlProps.get("sasl.mechanism"));
        putIfNotNull(props, "ssl.truststore.location", yamlProps.get("ssl.truststore.location"));
        putIfNotNull(props, "ssl.truststore.password", yamlProps.get("ssl.truststore.password"));

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
    public Chunk<?, ?, ?, ?> getChunk(ResultSet rs, TableMigrationContext ctx, Storage targetStorage) throws SQLException {
        return null;
    }

    @Override
    public void close() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void fulfillChunks(List<Config> configs, boolean sync, int rows) throws SQLException {

    }

    @Override
    public void preChecks(List<Config> configs) throws SQLException {

    }

    @Override
    public void createChunkTable() throws SQLException {

    }

    @Override
    public void dropChunkTable(List<Config> configs) throws SQLException {

    }

    private static class AvroRowProducer {
        private final KafkaProducer<String, byte[]> producer;
        private final Schema schema;
        private final List<FieldRuntimeContext> cachedFieldsContext;
        private final String firstFieldName;

        public AvroRowProducer(KafkaProducer<String, byte[]> producer, Schema schema, List<Column2Column> c2c) {
            this.producer = producer;
            this.schema = schema;

            this.cachedFieldsContext = c2c.stream().map(link -> {
                String dbLookupName = (link.sourceExpression() != null)
                        ? link.targetColumn().columnName()
                        : link.sourceColumn().columnName();

                String avroFieldName = link.targetColumn().columnName();
                String avroType = link.targetColumn().columnType();

                AvroTypeMapper mapper = TypeMapperFactory.getMapper(avroType);

                return new FieldRuntimeContext(avroFieldName, dbLookupName, mapper);
            }).toList();

            this.firstFieldName = c2c.get(0).targetColumn().columnName();
        }

        public LogMessage streamResultSetToKafka(Chunk<?,?,?,?> chunk, String topicName) throws SQLException, IOException {
            int rowCount = 0;

            ResultSet rs = (ResultSet) chunk.getResultSet();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            GenericRecord record = new GenericData.Record(schema);
            BinaryEncoder encoder = null;

            while (rs.next()) {
                for (FieldRuntimeContext context : cachedFieldsContext) {
                    Object value = context.mapper().getValue(rs, context.dbLookupName());
                    record.put(context.avroFieldName(), value);
                }

                out.reset();
                encoder = EncoderFactory.get().binaryEncoder(out, encoder);
                writer.write(record, encoder);
                encoder.flush();

                byte[] rowBytes = out.toByteArray();

                Object keyObj = record.get(firstFieldName);
                String messageKey = (keyObj != null) ? keyObj.toString() : String.valueOf(rowCount);

                producer.send(new ProducerRecord<>(topicName, messageKey, rowBytes));
                rowCount++;
            }

            producer.flush();

            chunk.setCopied(rowCount);
            return new LogMessage(chunk.getStartTime(), System.currentTimeMillis(), " To Kafka");
        }
    }

    @Override
    public void start(Storage targetStorage, List<Config> configs, int rows) throws SQLException {

    }

    @Override
    public void validate(Storage targetStorage, List<Config> configs) throws SQLException {

    }

    @Override
    public void createGlobalOutbox() throws SQLException {

    }

    @Override
    public <K, T, S extends AutoCloseable, R, V> void insertColumnValue(List<ColumnValue<V>> columnValues,
                                                                        Chunk<K, T, S, R> chunk) {
        try {
            String schemaName = chunk.getConfig().fromSchemaName();
            String tableName = chunk.getConfig().fromTableName();
            String globalCacheKey = schemaName + "." + tableName;

            ObjectRowProducer currentTableProcessor = valueCache.get(globalCacheKey);

            if (currentTableProcessor == null) {
                List<Column2Column> c2c = chunk.getT2t().column2Columns();
                Schema dynamicSchema = buildAvroSchemaFromC2C(c2c, chunk.getConfig());
                ObjectRowProducer newProducer = new ObjectRowProducer(dynamicSchema, c2c);

                ObjectRowProducer existing = valueCache.putIfAbsent(globalCacheKey, newProducer);
                currentTableProcessor = (existing != null) ? existing : newProducer;
            }

            currentTableProcessor.sendSingleRowToKafka(columnValues, topic);

        } catch (Exception e) {
            throw new RuntimeException("Failed to push parallel row to KafkaStorage", e);
        }
    }

    @Override
    public <K, T, S extends AutoCloseable, R, W> W getWriter(Chunk<K, T, S, R> chunk, String tableName) throws SQLException {
        return null;
    }

    private class ObjectRowProducer {
        private final Schema schema;
        private final Map<String, ObjectTypeMapper> cachedMappers = new ConcurrentHashMap<>();
        private final String firstFieldName;

        public ObjectRowProducer(Schema schema, List<Column2Column> c2c) {
            this.schema = schema;

            for (Column2Column link : c2c) {
                String avroFieldName = link.targetColumn().columnName().toLowerCase();
                String avroType = link.targetColumn().columnType();
                ObjectTypeMapper mapper = ObjectTypeMapperFactory.getMapper(avroType);
                this.cachedMappers.put(avroFieldName, mapper);
            }

            this.firstFieldName = c2c.get(0).targetColumn().columnName();
        }

        public <V> void sendSingleRowToKafka(List<ColumnValue<V>> columnValues, String topicName) throws IOException {
            GenericRecord record = new GenericData.Record(schema);

            for (ColumnValue<V> colValue : columnValues) {
                String avroFieldName = colValue.targetColumn().columnName();

                ObjectTypeMapper mapper = cachedMappers.get(avroFieldName.toLowerCase());
                if (mapper == null) {
                    continue;
                }

                Object rawValue = colValue.value();
                Object avroValue = mapper.convert(rawValue);

                record.put(avroFieldName, avroValue);
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            writer.write(record, encoder);
            encoder.flush();

            byte[] rowBytes = out.toByteArray();

            Object keyObj = record.get(firstFieldName);
            String messageKey = (keyObj != null) ? keyObj.toString() : UUID.randomUUID().toString();

            kafkaProducer.send(new ProducerRecord<>(topicName, messageKey, rowBytes));
        }
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void closeWriter(Chunk<K, T, S, R> chunk, String tableName) {

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
    public List<Chunk<?, ?, ?, ?>> getChunkList(List<TableMigrationContext> contexts, Storage targetStorage) throws SQLException {
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
        return new DummyTable(schemaName, tableName);
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
    public int getFetchSize() {
        return 0;
    }

    @Override
    public Table getDefaultSourceOutboxTable() {
        return new DummyTable.Builder("UNDEFINED","UNDEFINED").build();
    }

    @Override
    public Table getDefaultTargetOutboxTable() {
        return new DummyTable.Builder("UNDEFINED","UNDEFINED").build();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public <K, T, S extends AutoCloseable, R> void flushBuffer(Chunk<K, T, S, R> chunk) {

    }
}
