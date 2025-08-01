package org.bublik.model;

import org.bublik.service.NameSyntaxService;
import org.bublik.service.TableService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Table implements TableService, NameSyntaxService {
    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();
    private Integer id;
    private String schemaName;
    private String tableName;
    private List<Column> columns;
    private List<Column> pkColumns;
    private List<Index> indexes;
    private List<ForeignKey> foreignKeys;
    private List<TableOption> options;

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

    @Override
    public String getTaskName() {
        return getFinalTableName(false).toUpperCase() + "_TASK";
    }
}
