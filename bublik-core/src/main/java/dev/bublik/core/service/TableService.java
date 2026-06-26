package dev.bublik.core.service;

import dev.bublik.core.model.*;
import dev.bublik.core.storage.Storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface TableService {
    boolean exists(Connection connection) throws SQLException;
    String getFinalTableName(boolean withQuotes);
    String getFinalSchemaName();
    String getHintClause();
    String getTableTaskName();
    <S extends AutoCloseable> List<Column> getAllColumns(S connection) throws SQLException;
    List<Column> getPrimaryKeyColumns(Connection connection) throws SQLException;
    List<ForeignKey> getForeignKeys(Connection connection, Storage storage, Table targetTable) throws SQLException;
    List<Index> getTableIndexes(Connection connection) throws SQLException;
    List<UniqueConstraint> getUniqueConstraints(Connection connection) throws SQLException;
    Map.Entry<Integer, List<TableOption>> getOptions(Connection connection) throws SQLException;
    boolean hasPrimaryKey();
    void createPrimaryKey(Connection connection);
    void createIndexes(Connection connection);
    void createUniqueConstraints(Connection connection);
    void createForeignKeys(Connection connection);
    Map<String, String> getColumnToColumn(Connection connection) throws SQLException;
    void create(Connection connection) throws SQLException;
    <S extends AutoCloseable> boolean enrichTable(S session) throws SQLException;
    String buildOrderBy(Config config);
}
