-- TABLE1
BEGIN
  DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE1_TASK');
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => 'TABLE1_TASK',
                                               table_owner => 'OWNER',
                                               table_name  => 'TABLE1',
                                               by_row      => TRUE,
                                               chunk_size  => 20000);
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.drop_task('TABLE1_TASK');
END;
/
-- TABLE2
BEGIN
  DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE2_TASK');
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => 'TABLE2_TASK',
                                               table_owner => 'OWNER',
                                               table_name  => 'TABLE2',
                                               by_row      => TRUE,
                                               chunk_size  => 20000);
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.drop_task('TABLE2_TASK');
END;
/
-- TABLE3
BEGIN
  DBMS_PARALLEL_EXECUTE.create_task (task_name => 'TABLE3_TASK');
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => 'TABLE3_TASK',
                                               table_owner => 'OWNER',
                                               table_name  => 'TABLE3',
                                               by_row      => TRUE,
                                               chunk_size  => 20000);
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.drop_task('TABLE2_TASK');
END;
/
--
select * from user_parallel_execute_chunks;
