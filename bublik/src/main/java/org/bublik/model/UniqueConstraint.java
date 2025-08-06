package org.bublik.model;

import org.bublik.service.DDLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;

public class UniqueConstraint implements DDLService {
    private static final Logger log = LoggerFactory.getLogger(UniqueConstraint.class);

    private final String constraintName;
    private final Map<Short, Column> columns;
    private final boolean nullsNotDistinct;

    public UniqueConstraint(String constraintName, Map<Short, Column> columns, boolean nullsNotDistinct) {
        this.constraintName = constraintName;
        this.columns = columns;
        this.nullsNotDistinct = nullsNotDistinct;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public Map<Short, Column> getColumns() {
        return columns;
    }

    public boolean isNullsNotDistinct() {
        return nullsNotDistinct;
    }

    @Override
    public void create(Table table, Connection connection) {
        String sql = String.format(
                "alter table %s.%s add constraint %s unique %s (%s)",
                table.getSchemaName(),
                table.getFinalTableName(true),
                constraintName,
                nullsNotDistinct ? "nulls not distinct" : "",
                String.join(", ", columns.values().stream()
                        .map(Column::getColumnName)
                        .toList())
        );
        try {
            connection.createStatement().execute(sql);
            connection.commit();
        } catch (Exception e) {
            log.error("Failed to create unique constraint {} on table {}.{}: {}", constraintName, table.getSchemaName(), table.getFinalTableName(true), e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
