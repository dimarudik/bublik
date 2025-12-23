create or replace NONEDITIONABLE package dbms_parallel_execute_bublik AUTHID CURRENT_USER AS

  --
  -- Chunk status value
  --
  UNASSIGNED            CONSTANT NUMBER := 0;
  ASSIGNED              CONSTANT NUMBER := 1;
  PROCESSED             CONSTANT NUMBER := 2;
  PROCESSED_WITH_ERROR  CONSTANT NUMBER := 3;


  --
  -- Task Status value
  --
  CREATED               CONSTANT NUMBER := 1;
  CHUNKING              CONSTANT NUMBER := 2;
  CHUNKING_FAILED       CONSTANT NUMBER := 3;
  NO_CHUNKS             CONSTANT NUMBER := 4;
  CHUNKED               CONSTANT NUMBER := 5;
  PROCESSING            CONSTANT NUMBER := 6;
  FINISHED              CONSTANT NUMBER := 7;
  FINISHED_WITH_ERROR   CONSTANT NUMBER := 8;
  CRASHED               CONSTANT NUMBER := 9;


  --
  -- Exceptions
  --
  MISSING_ROLE              EXCEPTION;
    pragma exception_init(MISSING_ROLE,              -29490);
  INVALID_TABLE             EXCEPTION;
    pragma exception_init(INVALID_TABLE,             -29491);
  INVALID_STATE_FOR_CHUNK  EXCEPTION;
    pragma exception_init(INVALID_STATE_FOR_CHUNK,  -29492);
  INVALID_STATUS            EXCEPTION;
    pragma exception_init(INVALID_STATUS,            -29493);
  INVALID_STATE_FOR_RUN    EXCEPTION;
    pragma exception_init(INVALID_STATE_FOR_RUN,    -29494);
  INVALID_STATE_FOR_RESUME EXCEPTION;
    pragma exception_init(INVALID_STATE_FOR_RESUME, -29495);
  DUPLICATE_TASK_NAME      EXCEPTION;
    pragma exception_init(DUPLICATE_TASK_NAME,      -29497);
  TASK_NOT_FOUND           EXCEPTION;
    pragma exception_init(TASK_NOT_FOUND,           -29498);
  CHUNK_NOT_FOUND           EXCEPTION;
    pragma exception_init(CHUNK_NOT_FOUND,           -29499);

  -- Create/Drop Task Procedure
  function generate_task_name(prefix in varchar2 default 'TASK$_')
    return varchar2;

  procedure create_task(task_name  in varchar2,
                        comment    in varchar2 default null);

  procedure drop_task(task_name in varchar2);


  -- Create/Drop Chunks Procedures
  procedure create_chunks_by_rowid(task_name   in varchar2,
                                   table_owner in varchar2,
                                   table_name  in varchar2,
                                   by_row      in boolean,
                                   chunk_size  in number);

  procedure create_chunks_by_number_col(task_name    in varchar2,
                                        table_owner  in varchar2,
                                        table_name   in varchar2,
                                        table_column in varchar2,
                                        chunk_size   in number);

  procedure create_chunks_by_SQL(task_name in varchar2,
                                 sql_stmt  in clob,
                                 by_rowid  in boolean);

  procedure drop_chunks(task_name in varchar2);


  -- Individual Chunk retrieval and processing Procedures
  procedure get_rowid_chunk(task_name   in  varchar2,
                            chunk_id    out number,
                            start_rowid out rowid,
                            end_rowid   out rowid,
                            any_rows    out boolean);

  procedure get_number_col_chunk(task_name in  varchar2,
                                 chunk_id  out number,
                                 start_id  out number,
                                 end_id    out number,
                                 any_rows  out boolean);

  procedure set_chunk_status(task_name in varchar2,
                             chunk_id  in number,
                             status    in number,
                             err_num   in number   default null,
                             err_msg   in varchar2 default null);

  procedure purge_processed_chunks(task_name in varchar2);


  -- Task Status Retrieval Procesure
  function task_status(task_name in varchar2) return number;


  -- Parallel Execution procedure: run, stop, resume
  procedure run_task(
    task_name                  in varchar2,
    sql_stmt                   in clob,
    language_flag              in number,
    edition                    in varchar2 default NULL,
    apply_crossedition_trigger in varchar2 default NULL,
    fire_apply_trigger         in boolean  default TRUE,
    parallel_level             in number   default 0,
    job_class                  in varchar2 default 'DEFAULT_JOB_CLASS');

  procedure resume_task(
    task_name                  in varchar2,
    sql_stmt                   in clob,
    language_flag              in number,
    edition                    in varchar2 default NULL,
    apply_crossedition_trigger in varchar2 default NULL,
    fire_apply_trigger         in boolean  default TRUE,
    parallel_level             in number   default 0,
    job_class                  in varchar2 default 'DEFAULT_JOB_CLASS',
    force                      in boolean  default FALSE);

  procedure resume_task(task_name in varchar2,
                        force     in boolean default FALSE);

  procedure stop_task(task_name in varchar2);


  -- CUSTOMER SHOULD NOT use this one.
  -- This is an internal routine for parallel execution.
  procedure run_internal_worker(task_name in varchar2,
                                job_name  in varchar2);


  -- Administrative Procedure
  --   All of the following subroutines requires ADM_PARALLEL_EXECUTE role.
  procedure adm_drop_task(task_owner in varchar2,
                          task_name  in varchar2);

  procedure adm_drop_chunks(task_owner in varchar2,
                            task_name  in varchar2);

  function adm_task_status(task_owner in varchar2,
                           task_name  in varchar2) return number;

  procedure adm_stop_task(task_owner in varchar2,
                          task_name  in varchar2);

end;
/

