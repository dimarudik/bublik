package org.example.service;

import org.example.model.Chunk;
import org.example.model.OraChunk;
import org.example.model.PGChunk;

import java.sql.*;

import static org.example.constants.SQLConstants.DML_UPDATE_STATUS_CTID_CHUNKS;
import static org.example.constants.SQLConstants.DML_UPDATE_STATUS_ROWID_CHUNKS;

public class ChunkServiceImpl implements ChunkService {
    @Override
    public void markChunkAsProceed(Chunk chunk, Connection connection) throws SQLException {
        if (chunk instanceof OraChunk) {
            CallableStatement callableStatement =
                    connection.prepareCall(DML_UPDATE_STATUS_ROWID_CHUNKS);
            callableStatement.setString(1, chunk.config().fromTaskName());
            callableStatement.setInt(2, chunk.chunkId());
            callableStatement.execute();
            callableStatement.close();
        } else if (chunk instanceof PGChunk) {
            PreparedStatement updateStatus = connection.prepareStatement(DML_UPDATE_STATUS_CTID_CHUNKS);
            updateStatus.setLong(1, chunk.chunkId());
            updateStatus.setString(2, chunk.config().fromTaskName());
            int rows = updateStatus.executeUpdate();
            updateStatus.close();
        }
    }

    @Override
    public ResultSet getChunkOfData(Chunk chunk, Connection connection, String query) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        if (chunk instanceof OraChunk) {
            return fetchOraResultSet(chunk, preparedStatement);
        } else if (chunk instanceof PGChunk) {
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
