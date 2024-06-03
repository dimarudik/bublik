package org.example.exception;

import java.sql.SQLException;

public class TableNotExistsException extends SQLException {
    public TableNotExistsException(String errorMessage){
        super(errorMessage);
    }

    public TableNotExistsException(String schemaName, String tableName) {
        super("Table " + schemaName + "." + tableName + " doesn't exists");
    }
}
