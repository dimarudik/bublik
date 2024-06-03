package org.bublik.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bublik.service.SQLSyntaxService;
import org.bublik.service.TableService;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
@Getter
public abstract class Table implements TableService, SQLSyntaxService {
    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();
    private String schemaName;
    private String tableName;

    public static Set<String> tableExistsCache() {
        return Table.tableExistsCache;
    }
}
