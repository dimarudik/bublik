package org.bublik.model;

import java.util.List;

public class ForeignKey {
    private final Table fkTable;
    private final List<Column> fkColumns;
    private Table pkTable;
    private List<Column> pkColumns;
    private final String pkName;
    private final String fkName;
    private final String updateRule;
    private final String deleteRule;
    private final short deferrability;

    public ForeignKey(Table fkTable, List<Column> fkColumns, String pkName, String fkName, String updateRule, String deleteRule, short deferrability) {
        this.fkTable = fkTable;
        this.fkColumns = fkColumns;
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

    public void setPkTable(Table pkTable) {
        this.pkTable = pkTable;
    }

    public void setPkColumns(List<Column> pkColumns) {
        this.pkColumns = pkColumns;
    }
}
