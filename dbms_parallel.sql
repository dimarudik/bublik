-- APPLICATIONONDEVICE
BEGIN
  DBMS_PARALLEL_EXECUTE.create_task (task_name => 'APPLICATIONONDEVICE_TASK');
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => 'APPLICATIONONDEVICE_TASK',
                                               table_owner => 'ESBCOMMON',
                                               table_name  => 'APPLICATIONONDEVICE',
                                               by_row      => TRUE,
                                               chunk_size  => 20000);
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.drop_task('APPLICATIONONDEVICE_TASK');
END;
/
-- DEVICE
BEGIN
  DBMS_PARALLEL_EXECUTE.create_task (task_name => 'DEVICE_TASK');
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => 'DEVICE_TASK',
                                               table_owner => 'ESBCOMMON',
                                               table_name  => 'DEVICE',
                                               by_row      => TRUE,
                                               chunk_size  => 20000);
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.drop_task('DEVICE_TASK');
END;
/
-- APPLICATION
BEGIN
  DBMS_PARALLEL_EXECUTE.create_task (task_name => 'APPLICATION_TASK');
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => 'APPLICATION_TASK',
                                               table_owner => 'ESBCOMMON',
                                               table_name  => 'APPLICATION',
                                               by_row      => TRUE,
                                               chunk_size  => 20000);
END;
/
BEGIN
  DBMS_PARALLEL_EXECUTE.drop_task('DEVICE_TASK');
END;
/
--
select * from user_parallel_execute_chunks;
