package dev.bublik.core.service;

import dev.bublik.core.model.Table;

import java.sql.Connection;

public interface DDLService {
    void create(Table table, Connection connection);
}
