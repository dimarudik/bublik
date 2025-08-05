package org.bublik.service;

import org.bublik.model.Table;

import java.sql.Connection;

public interface IndexService {
    void createIndex(Table table, Connection connection);
}
