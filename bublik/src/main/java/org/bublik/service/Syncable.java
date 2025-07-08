package org.bublik.service;

import org.bublik.model.PGChunk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface Syncable {
    Map<Integer, PGChunk<?>> getChunkSyncMap(Connection connection) throws SQLException;
}
