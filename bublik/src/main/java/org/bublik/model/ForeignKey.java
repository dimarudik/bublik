package org.bublik.model;

import org.bublik.service.DDLService;
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
        // Implementation for creating the foreign key in the database
        // This method should contain the SQL logic to create the foreign key constraint
        // using the properties of this ForeignKey object.
        // Example:
        // String sql = "ALTER TABLE " + fkTable.getFinalTableName(true) +
        //              " ADD CONSTRAINT " + fkName +
        //              " FOREIGN KEY (" + String.join(", ", fkColumns.stream().map(Column::getName).toList()) + ")" +
        //              " REFERENCES " + pkTable.getFinalTableName(true) +
        //              " (" + String.join(", ", pkColumns.stream().map(Column::getName).toList()) + ")" +
        //              " ON UPDATE " + updateRule +
        //              " ON DELETE " + deleteRule;
        // Execute this SQL statement using the provided connection.
        // Note: Ensure to handle exceptions and manage transactions as needed.
        // This is a placeholder for the actual SQL execution logic.
        String sql = String.format(
            "ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) " +
            "ON UPDATE %s ON DELETE %s DEFERRABLE %s",
            fkTable.getSchemaName(),
            fkTable.getTableName(),
            fkName,
            String.join(", ", fkColumns.stream().map(Column::getColumnName).toList()),
            pkTable.getTableName(),
            String.join(", ", pkColumns.stream().map(Column::getColumnName).toList()),
            updateRule,
            deleteRule,
            deferrability == 1 ? "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE"
        );
        log.info("Creating foreign key with SQL: {}", sql);
    }
}
