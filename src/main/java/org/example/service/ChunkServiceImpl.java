package org.example.service;

import org.example.constants.SourceContextHolder;
import org.example.model.Chunk;

import java.sql.*;

import static org.example.constants.SQLConstants.*;

public class ChunkServiceImpl implements ChunkService {
    @Override
    public void markChunkAsProceed(Chunk chunk, Connection connection, SourceContextHolder contextHolder) throws SQLException {
        if (contextHolder.sourceContext().toString().equals(LABEL_ORACLE)) {
            CallableStatement callableStatement =
                    connection.prepareCall(DML_UPDATE_STATUS_ROWID_CHUNKS);
            callableStatement.setString(1, chunk.config().fromTaskName());
            callableStatement.setInt(2, chunk.chunkId());
            callableStatement.execute();
            callableStatement.close();
        } else if (contextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)) {
            PreparedStatement updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS);
            updateStatus.setLong(1, chunk.chunkId());
            updateStatus.setString(2, chunk.config().fromTaskName());
            int rows = updateStatus.executeUpdate();
            updateStatus.close();
        }
    }

    @Override
    public ResultSet getChunkOfData(Chunk chunk, Connection connection, SourceContextHolder contextHolder, String query) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        if (contextHolder.sourceContext().toString().equals(LABEL_ORACLE)) {
            return fetchOraResultSet(chunk, preparedStatement);
        } else if (contextHolder.sourceContext().toString().equals(LABEL_POSTGRESQL)){
            return preparedStatement.executeQuery();
        }
        return null;
    }

    private ResultSet fetchOraResultSet(Chunk chunk, PreparedStatement statement) throws SQLException {
        if (chunk.config().numberColumn() == null) {
            statement.setString(1, chunk.startRowId());
            statement.setString(2, chunk.endRowId());
        } else {
            statement.setLong(1, chunk.startId());
            statement.setLong(2, chunk.endId());
        }
        return statement.executeQuery();
    }
}
