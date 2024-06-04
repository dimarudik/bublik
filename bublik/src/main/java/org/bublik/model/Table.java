package org.bublik.model;

import org.bublik.service.SQLSyntaxService;
import org.bublik.service.TableService;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Table implements TableService, SQLSyntaxService {
    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();
    private final String schemaName;
    private final String tableName;

    public Table(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public static Set<String> tableExistsCache() {
        return Table.tableExistsCache;
    }
}
