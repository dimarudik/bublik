package org.bublik.service;

import org.bublik.constants.ChunkStatus;
import org.bublik.model.PGChunk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface Syncable {
    Map<Integer, PGChunk<?>> getChunkSyncMap(Connection connection, List<ChunkStatus> chunkStatuses) throws SQLException;
}
