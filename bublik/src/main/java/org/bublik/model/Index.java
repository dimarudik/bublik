package org.bublik.model;

import java.util.Map;

public class Index {
    private final Table table;
    private final String indexName;
    private final Map<Short, Column> columns;
    private final boolean isNonUnique;
    private final String ascOrDesc;
    private final String filterCondition;
    private final String indexDef;

    public Index(Table table, String indexName, Map<Short, Column> columns, boolean isNonUnique,
                 String ascOrDesc, String filterCondition, String indexDef) {
        this.table = table;
        this.indexName = indexName;
        this.columns = columns;
        this.isNonUnique = isNonUnique;
        this.ascOrDesc = ascOrDesc;
        this.filterCondition = filterCondition;
        this.indexDef = indexDef;
    }

    public Table getTable() {
        return table;
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<Short, Column> getColumns() {
        return columns;
    }

    public boolean isNonUnique() {
        return isNonUnique;
    }

    public String getAscOrDesc() {
        return ascOrDesc;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public String getIndexDef() {
        return indexDef;
    }
}
