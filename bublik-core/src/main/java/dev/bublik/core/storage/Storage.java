package dev.bublik.core.storage;

import dev.bublik.core.model.Column;
import dev.bublik.core.model.ConnectionProperty;
import dev.bublik.core.model.Table;
import dev.bublik.core.service.StorageService;

import java.sql.Wrapper;
import java.util.List;
import java.util.Map;

public abstract class Storage implements StorageService, Wrapper, AutoCloseable {
    private final StorageClass storageClass;
    protected int threadCount;
    private final ConnectionProperty connectionProperty;
    private final Table outboxTable;
    private Map<Table, Table> tables;

    public Storage(ConnectionProperty connectionProperty) {
        this(null, connectionProperty, null);
    }

    public Storage(ConnectionProperty connectionProperty, Table outboxTable) {
        this(null, connectionProperty, outboxTable);
    }

    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty) {
        this(storageClass, connectionProperty, null);
    }

    protected Storage(StorageClass storageClass, ConnectionProperty connectionProperty, Table outboxTable) {
        this.storageClass = storageClass;
        this.connectionProperty = connectionProperty;
        this.outboxTable = outboxTable;
    }

    public Map<Table, Table> getTables() {
        return tables;
    }

    public void setTables(Map<Table, Table> tables) {
        this.tables = tables;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ConnectionProperty getConnectionProperty() {
        return connectionProperty;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public Table getOutboxTable() {
        return outboxTable;
    }

    public Column columnFromAvro(Map<String, Object> avroSchema, String avroFieldName, int position) {
        if (avroSchema == null || !avroSchema.containsKey("fields")) {
            throw new IllegalArgumentException("Invalid avroSchema structure: 'fields' block not found.");
        }

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");

        Map<String, Object> avroField = fields.stream()
                .filter(f -> avroFieldName.equalsIgnoreCase(String.valueOf(f.get("name"))))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Field '" + avroFieldName + "' not found in avroSchema configuration"));

        Object typeObj = avroField.get("type");
        String columnType = "string";
        int isNullable = 1;

        if (typeObj instanceof List) {
            List<?> typeList = (List<?>) typeObj;
            isNullable = typeList.contains("null") ? 1 : 0;

            columnType = typeList.stream()
                    .map(String::valueOf)
                    .filter(t -> !"null".equalsIgnoreCase(t))
                    .findFirst()
                    .orElse("string");
        } else if (typeObj instanceof String) {
            columnType = (String) typeObj;
            isNullable = "null".equalsIgnoreCase(columnType) ? 1 : 0;
        }

        Object defaultObj = avroField.get("default");
        String defaultValue = (defaultObj != null) ? defaultObj.toString() : null;

        int dataType = java.sql.Types.VARCHAR; // Дефолт
        dataType = switch (columnType.toLowerCase()) {
            case "int" -> java.sql.Types.INTEGER;
            case "long" -> java.sql.Types.BIGINT;
            case "double" -> java.sql.Types.DOUBLE;
            case "float" -> java.sql.Types.FLOAT;
            case "boolean" -> java.sql.Types.BOOLEAN;
            case "bytes" -> java.sql.Types.BLOB;
            default -> dataType;
        };

        return new Column(
                position,
                avroFieldName,
                columnType,
                dataType,
                isNullable,
                defaultValue
        );
    }
}
