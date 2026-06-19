package dev.bublik.core.model;

import java.util.Comparator;
import java.util.List;

public record Table2Table<S extends AutoCloseable> (Table<S> sourceTable,
                                                    Table<S> targetTable,
                                                    List<Column2Column> column2Columns,
                                                    Column ttlColumn,
                                                    Column timestampColumn) {

    public List<Column2Column> getSortedColumn2ColumnByTargetColumnPosition() {
        return column2Columns.stream()
                .sorted(Comparator.comparingInt(c2c -> c2c.targetColumn().columnPosition()))
                .toList();
    }
}
