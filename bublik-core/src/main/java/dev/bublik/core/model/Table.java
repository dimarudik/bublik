package dev.bublik.core.model;

import dev.bublik.core.service.NameSyntaxService;
import dev.bublik.core.service.TableService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Table implements TableService, NameSyntaxService, Comparable<Table> {
    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();
    private Integer id;
    private final String schemaName;
    private final String tableName;
    private List<Column> columns;
    private List<Column> pkColumns;
    private List<Index> indexes;
    private List<ForeignKey> foreignKeys;
    private List<TableOption> options;
    private List<UniqueConstraint> uniqueConstraints;

//    public Table() {}

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
        if (!(o instanceof Table table)) return false;
        return schemaName.equals(table.schemaName) && tableName.equals(table.tableName);
    }

    @Override
    public int compareTo(Table o) {
        return (this.getSchemaName() + "." + this.getTableName()).compareTo(o.getSchemaName() + "." + o.getTableName());
    }

    public String getTableFullName() {
        return getSchemaName() + "." + getTableName();
    }

    public String getTableNameWithoutQuotes() {
        return getWordWithoutQuotes(tableName);
    }

    public String tableToString() {
        if (getSchemaName() == null) {
            return getTableName();
        } else {
            return getSchemaName() + "." + getTableName();
        }
    }

    protected Table(Builder<?, ?> builder) {
        this.id = builder.id;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.columns = builder.columns;
        this.pkColumns = builder.pkColumns;
        this.indexes = builder.indexes;
        this.foreignKeys = builder.foreignKeys;
        this.options = builder.options;
        this.uniqueConstraints = builder.uniqueConstraints;
    }

    protected static abstract class Builder<C extends Table, B extends Builder<C, B>> {
        protected Integer id;
        protected final String schemaName;
        protected final String tableName;
        protected List<Column> columns = new ArrayList<>();
        protected List<Column> pkColumns = new ArrayList<>();
        protected List<Index> indexes = new ArrayList<>();
        protected List<ForeignKey> foreignKeys = new ArrayList<>();
        protected List<TableOption> options = new ArrayList<>();
        protected List<UniqueConstraint> uniqueConstraints = new ArrayList<>();

        protected Builder(String schemaName, String tableName) {
            if (schemaName == null || schemaName.isBlank()) {
                throw new IllegalArgumentException("Schema name cannot be null or empty");
            }
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("Table name cannot be null or empty");
            }
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        protected abstract B self();
        public abstract C build();

        public B id(Integer id) { this.id = id; return self(); }

        public B columns(List<Column> columns) { this.columns = columns; return self(); }
        public B pkColumns(List<Column> pkColumns) { this.pkColumns = pkColumns; return self(); }
        public B indexes(List<Index> indexes) { this.indexes = indexes; return self(); }
        public B foreignKeys(List<ForeignKey> foreignKeys) { this.foreignKeys = foreignKeys; return self(); }
        public B options(List<TableOption> options) { this.options = options; return self(); }
        public B uniqueConstraints(List<UniqueConstraint> uniqueConstraints) { this.uniqueConstraints = uniqueConstraints; return self(); }

        public B addColumn(Column column) { this.columns.add(column); return self(); }
        public B addPkColumn(Column column) { this.pkColumns.add(column); return self(); }
        public B addIndex(Index index) { this.indexes.add(index); return self(); }
        public B addForeignKey(ForeignKey fk) { this.foreignKeys.add(fk); return self(); }

        protected void validate() {
            if (schemaName == null || schemaName.isBlank()) throw new IllegalStateException("Schema name must not be empty");
            if (tableName == null || tableName.isBlank()) throw new IllegalStateException("Table name must not be empty");
        }
    }
}
