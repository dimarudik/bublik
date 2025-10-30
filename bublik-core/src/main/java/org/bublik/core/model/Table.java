package org.bublik.core.model;

import org.bublik.core.service.NameSyntaxService;
import org.bublik.core.service.TableService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Table<S extends AutoCloseable> implements TableService<S>, NameSyntaxService {
    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();
    private Integer id;
    private String schemaName;
    private String tableName;
    private List<Column> columns;
    private List<Column> pkColumns;
    private List<Index> indexes;
    private List<ForeignKey> foreignKeys;
    private List<TableOption> options;
    private List<UniqueConstraint> uniqueConstraints;

    public Table() {}

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

    public List<Column> getPkColumns() {
        return pkColumns;
    }

    public void setPkColumns(List<Column> pkColumns) {
        this.pkColumns = pkColumns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<Index> indexes) {
        this.indexes = indexes;
    }

    public List<TableOption> getOptions() {
        return options;
    }

    public void setOptions(List<TableOption> options) {
        this.options = options;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public List<UniqueConstraint> getUniqueConstraints() {
        return uniqueConstraints;
    }

    public void setUniqueConstraints(List<UniqueConstraint> uniqueConstraints) {
        this.uniqueConstraints = uniqueConstraints;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(List<ForeignKey> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    @Override
    public String getTableTaskName() {
        return getFinalTableName(false).toUpperCase() + "_TASK";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Table<?> table)) return false;
        return schemaName.equals(table.schemaName) && tableName.equals(table.tableName);
    }

    public String getTableFullName() {
        return getSchemaName() + "." + getTableName();
    }
}
