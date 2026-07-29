package dev.bublik.oracle.model;

import dev.bublik.core.constants.ChunkStatus;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.Table2Table;
import dev.bublik.core.storage.Storage;

import java.sql.*;

public class OraChunkNumberCol<K extends Integer, T extends Long, S extends Connection, R extends ResultSet>
        extends OraChunk<K, T, S, R> {

    public OraChunkNumberCol(K id, T start, T end, Config config, Table2Table t2t,
                         ChunkStatus status, String fetchQuery, Storage sourceStorage,
                         Storage targetStorage, String orderByClause) {
        super(id, start, end, config, t2t, status, fetchQuery, sourceStorage, targetStorage, orderByClause);
    }

    @Override
    public ResultSet getData(String query) throws SQLException {
        Connection connection = getSourceSession();
        String sql = query + getOrderByClause();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setLong(1, getStart());
        statement.setLong(2, getEnd());
        statement.setFetchSize(getSourceStorage().getFetchSize());
        return statement.executeQuery();
    }
}
