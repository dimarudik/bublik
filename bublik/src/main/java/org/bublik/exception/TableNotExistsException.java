package org.bublik.exception;

import java.sql.SQLException;

public class TableNotExistsException extends SQLException {
    public TableNotExistsException(String errorMessage){
        super(errorMessage);
    }

    public TableNotExistsException(String schemaName, String tableName) {
        super("\u001B[31mTable " + schemaName + "." + tableName + " doesn't exists\u001B[0m");
    }
}
