package org.example.util;

import org.example.model.Config;

import java.util.*;

public class SQLUtil {

    public static String buildStartEndRowIdOfOracleChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select rownum, chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from (" +
                "select chunk_id, start_rowid, end_rowid, start_id, end_id, task_name from user_parallel_execute_chunks where task_name in ('" +
                 String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' " +
                "order by ora_hash(concat(task_name,start_rowid)) ) order by 1";
    }

    public static String buildStartEndPageOfPGChunk(List<Config> configs) {
        List<String> taskNames = new ArrayList<>();
        configs.forEach(sqlStatement -> taskNames.add(sqlStatement.fromTaskName()));
        return "select row_number() over (order by chunk_id) as rownum, chunk_id, start_page, end_page, task_name from public.ctid_chunks where task_name in ('" +
                String.join("', '", taskNames) + "') " +
                "and status <> 'PROCESSED' ";
    }
}
