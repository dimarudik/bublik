package org.bublik.service;

import org.bublik.model.Table;

import java.sql.Connection;

public interface DDLService {
    void create(Table table, Connection connection);
}
