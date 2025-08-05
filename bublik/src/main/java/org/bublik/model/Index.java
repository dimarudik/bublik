package org.bublik.model;

import org.bublik.service.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;

public class Index implements IndexService {
    private static final Logger log = LoggerFactory.getLogger(Index.class);

    private final Integer id;
    private final String indexName;
    private final Map<Short, Column> columns;
    private final Map<Short, Column> includeColumns;
    private final boolean isUnique;
    private final String filterCondition;
    private final String indexDef;

    public Index(Integer id, String indexName, Map<Short, Column> columns, Map<Short, Column> includeColumns,
                 boolean isUnique, String filterCondition, String indexDef) {
        this.id = id;
        this.indexName = indexName;
        this.columns = columns;
        this.includeColumns = includeColumns;
        this.isUnique = isUnique;
        this.filterCondition = filterCondition;
        this.indexDef = indexDef;
    }

    public Integer getId() {
        return id;
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<Short, Column> getColumns() {
        return columns;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public String getIndexDef() {
        return indexDef;
    }

    public Map<Short, Column> getIncludeColumns() {
        return includeColumns;
    }

    @Override
    public void createIndex(Table table, Connection connection) {
        String sqlInclude = includeColumns.isEmpty() ? "" : " INCLUDE (" +
                String.join(", ", includeColumns.values().stream().map(Column::getColumnName).toList()) + ")";
        String sql = String.format(
                "CREATE %s INDEX %s ON %s.%s (%s) %s %s",
                isUnique ? "UNIQUE" : "",
                "",
//                indexName,
                table.getSchemaName(),
                table.getFinalTableName(true),
                String.join(", ", columns.values().stream().map(Column::getColumnNameWithAscOrDesc).toList()),
                sqlInclude,
                filterCondition != null ? "WHERE " + filterCondition : ""
        );
        log.info("{}", sql);
        try {
            connection.createStatement().execute(sql);
            connection.commit();
        } catch (Exception e) {
            log.error("Failed to create index {} on table {}.{}: {}", indexName, table.getSchemaName(), table.getTableName(), e.getMessage());
            throw new RuntimeException("Error creating index: " + e.getMessage(), e);
        }
    }
}
