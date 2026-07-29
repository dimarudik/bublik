package dev.bublik.oracle.model;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.Table2Table;
import dev.bublik.core.storage.Storage;

import java.sql.*;

public class OraChunkRowId<K extends Integer, T extends RowId, S extends Connection, R extends ResultSet>
        extends OraChunk<K, T, S, R> {

    public OraChunkRowId(K id, T start, T end, Config config, Table2Table t2t,
                         ChunkStatus status, String fetchQuery, Storage sourceStorage,
                         Storage targetStorage, String orderByClause) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage, orderByClause);
    }

    @Override
    public ResultSet getData(String query) throws SQLException {
        Connection connection = getSourceSession();
        String sql = query + getOrderByClause();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setRowId(1, getStart());
        statement.setRowId(2, getEnd());
        statement.setFetchSize(getSourceStorage().getFetchSize());
        return statement.executeQuery();
    }
}
