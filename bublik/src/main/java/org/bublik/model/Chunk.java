package org.bublik.model;

import org.bublik.service.ChunkService;
import org.bublik.storage.JDBCStorage;
import org.bublik.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.bublik.exception.Utils.getStackTrace;

public abstract class Chunk<T> implements ChunkService {
    private static final Logger LOGGER = LoggerFactory.getLogger(Chunk.class);

    private final Integer id;
    private final T start;
    private final T end;
    private final Config config;
    private final Table sourceTable;
    private Table targetTable;
    private final Storage sourceStorage;
    private Storage targetStorage;
    private Connection sourceConnection;
    private LogMessage logMessage;
    private ResultSet resultSet;

    public Chunk(Integer id, T start, T end, Config config, Table sourceTable,
                 Storage sourceStorage) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.config = config;
        this.sourceTable = sourceTable;
        this.sourceStorage = sourceStorage;
    }

    public Integer getId() {
        return id;
    }

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }

    public Config getConfig() {
        return config;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public Storage getSourceStorage() {
        return sourceStorage;
    }

    public void setTargetTable(Table table) {
        this.targetTable = table;
    }

    public Storage getTargetStorage() {
        return targetStorage;
    }

    public Connection getSourceConnection() {
        return sourceConnection;
    }

    public void setSourceConnection(Connection sourceConnection) {
        this.sourceConnection = sourceConnection;
    }

    public LogMessage getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(LogMessage logMessage) {
        this.logMessage = logMessage;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public void setTargetStorage(Storage targetStorage) {
        this.targetStorage = targetStorage;
    }

    public Chunk<?> assignSourceConnection() {
        if (getSourceStorage() instanceof JDBCStorage) {
            try {
                Connection sourceConnection = getSourceStorage().getConnection();
                setSourceConnection(sourceConnection);
                return this;
            } catch (SQLException | RuntimeException e) {
                LOGGER.error("Source Connection:\n {}", getStackTrace(e));
            }
        }
        return null;
    }

    public Chunk<?> assignSourceResultSet() throws SQLException {
        try {
            String s = buildFetchStatement();
            ResultSet resultSet = getData(getSourceConnection(), s);
            setResultSet(resultSet);
//            System.out.println(s);
            return this;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", getStackTrace(e));
            throw e;
        }
    }

    public Chunk<?> assignResultLogMessage() throws SQLException {
        try {
            LogMessage logMessage = this.getTargetStorage().transferToTarget(this);
            this.setLogMessage(logMessage);
            ResultSet resultSet = this.getResultSet();
            resultSet.close();
            return this;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", getStackTrace(e));
            this.setLogMessage(new LogMessage (0, 0, 0, " UNREACHABLE TASK ", this));
            throw e;
//            return this;
        }
    }

    public Chunk<?> closeChunkSourceConnection() {
        try {
            Connection connection = getSourceConnection();
            connection.close();
            return this;
        } catch (SQLException | RuntimeException e) {
            LOGGER.error("{}", getStackTrace(e));
            throw new RuntimeException();
        }
    }
}
