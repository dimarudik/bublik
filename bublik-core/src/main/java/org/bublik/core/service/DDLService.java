package org.bublik.core.service;

import org.bublik.core.model.Table;

import java.sql.Connection;

public interface DDLService {
    void create(Table table, Connection connection);
}
