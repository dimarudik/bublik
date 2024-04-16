package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.service.SQLSyntaxService;
import org.example.service.TableService;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@NoArgsConstructor
@AllArgsConstructor
@Data
public abstract class Table implements TableService, SQLSyntaxService {
    private static final Set<String> tableExistsCache = ConcurrentHashMap.newKeySet();
    private String schemaName;
    private String tableName;

    public static Set<String> tableExistsCache() {
        return Table.tableExistsCache;
    }
}
