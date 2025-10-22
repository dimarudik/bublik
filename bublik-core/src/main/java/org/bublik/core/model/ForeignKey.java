package org.bublik.core.model;

import org.bublik.core.service.DDLService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

public class ForeignKey implements DDLService {
    private static final Logger log = LoggerFactory.getLogger(ForeignKey.class);

    private final Table fkTable;
    private final List<Column> fkColumns;
    private final Table pkTable;
    private final List<Column> pkColumns;
    private final String pkName;
    private final String fkName;
    private final String updateRule;
    private final String deleteRule;
    private final short deferrability;

    public ForeignKey(Table fkTable, List<Column> fkColumns, Table pkTable, List<Column> pkColumns, String pkName,
                      String fkName, String updateRule, String deleteRule, short deferrability) {
        this.fkTable = fkTable;
        this.fkColumns = fkColumns;
        this.pkTable = pkTable;
        this.pkColumns = pkColumns;
        this.pkName = pkName;
        this.fkName = fkName;
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
        this.deferrability = deferrability;
    }

    public Table getFkTable() {
        return fkTable;
    }

    public List<Column> getFkColumns() {
        return fkColumns;
    }

    public Table getPkTable() {
        return pkTable;
    }

    public List<Column> getPkColumns() {
        return pkColumns;
    }

    public String getPkName() {
        return pkName;
    }

    public String getFkName() {
        return fkName;
    }

    public String getUpdateRule() {
        return updateRule;
    }

    public String getDeleteRule() {
        return deleteRule;
    }

    public short getDeferrability() {
        return deferrability;
    }

    @Override
    public void create(Table table, Connection connection) {
        String sql = String.format(
            "ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ",
            fkTable.getSchemaName(),
            fkTable.getTableName(),
            fkName,
            String.join(", ", fkColumns.stream().map(Column::columnName).toList()),
            pkTable.getSchemaName(),
            pkTable.getTableName(),
            String.join(", ", pkColumns.stream().map(Column::columnName).toList())
        );
        log.info("{}", sql);
        try {
            connection.createStatement().execute(sql);
            connection.commit();
        } catch (Exception e) {
            log.error("Failed to create foreign key {} on table {}.{}: {}", fkName, fkTable.getSchemaName(), fkTable.getTableName(), e.getMessage());
            throw new RuntimeException("Error creating foreign key: " + e.getMessage(), e);
        }
    }
}
