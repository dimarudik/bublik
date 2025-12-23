create NONEDITIONABLE PACKAGE dbms_parallel_execute_i_b AS


  SUBTYPE TASK_TYPE  IS DBMS_PARALLEL_EXECUTE_TASK$%ROWTYPE;






  UNDECLARED            CONSTANT NUMBER := -1;
  ROWID_RANGE           CONSTANT NUMBER := 0;
  NUMBER_RANGE          CONSTANT NUMBER := 1;



  FUNCTION GENERATE_TASK_NAME(PREFIX IN VARCHAR2 DEFAULT 'TASK$_')
    RETURN VARCHAR2;

  PROCEDURE CREATE_TASK(OWNER#     IN NUMBER,
                        TASK_NAME  IN VARCHAR2,
                        COMMENT    IN VARCHAR2 DEFAULT NULL);

  PROCEDURE DROP_TASK(OWNER#    IN NUMBER,
                      TASK_NAME IN VARCHAR2);



  PROCEDURE CREATE_CHUNKS_BY_ROWID(OWNER#      IN NUMBER,
                                   TASK_NAME   IN VARCHAR2,
                                   TABLE_OWNER IN VARCHAR2,
                                   TABLE_NAME  IN VARCHAR2,
                                   NUM_ROW     IN NUMBER);

  PROCEDURE CREATE_CHUNKS_BY_ROWID(OWNER#      IN NUMBER,
                                   TASK_NAME   IN VARCHAR2,
                                   TABLE_OWNER IN VARCHAR2,
                                   TABLE_NAME  IN VARCHAR2,
                                   NUM_BLOCK   IN NUMBER);

  PROCEDURE CREATE_CHUNKS_BY_NUMBER_COL(OWNER#       IN NUMBER,
                                        TASK_NAME    IN VARCHAR2,
                                        TABLE_OWNER  IN VARCHAR2,
                                        TABLE_NAME   IN VARCHAR2,
                                        TABLE_COLUMN IN VARCHAR2,
                                        CHUNK_SIZE   IN NUMBER);

  PROCEDURE CREATE_CHUNKS_BY_SQL(OWNER#    IN NUMBER,
                                 TASK_NAME IN VARCHAR2,
                                 SQL_STMT  IN CLOB,
                                 BY_ROWID  IN BOOLEAN);

  PROCEDURE DROP_CHUNKS(OWNER#    IN NUMBER,
                        TASK_NAME IN VARCHAR2);



  FUNCTION GET_RANGE(OWNER#      IN  NUMBER,
                     TASK_NAME   IN  VARCHAR2,
                     CHUNK_ID    OUT NUMBER,
                     START_ROWID OUT ROWID,
                     END_ROWID   OUT ROWID,
                     START_ID    OUT NUMBER,
                     END_ID      OUT NUMBER) RETURN BOOLEAN;

  PROCEDURE SET_CHUNK_STATUS(OWNER#    IN NUMBER,
                             TASK_NAME IN VARCHAR2,
                             CHUNK_ID  IN NUMBER,
                             STATUS    IN NUMBER,
                             ERR_NUM   IN NUMBER   DEFAULT NULL,
                             ERR_MSG   IN VARCHAR2 DEFAULT NULL);

  PROCEDURE UNASSIGN_CHUNKS(OWNER#    IN NUMBER,
                            TASK_NAME IN VARCHAR2);

  PROCEDURE PURGE_PROCESSED_CHUNKS(OWNER#    IN NUMBER,
                                   TASK_NAME IN VARCHAR2);



  FUNCTION TASK_STATUS(OWNER#    IN NUMBER,
                       TASK_NAME IN VARCHAR2) RETURN NUMBER;



  FUNCTION READ_TASK(OWNER# IN NUMBER,
                     TASK   IN VARCHAR2) RETURN TASK_TYPE;

  PROCEDURE UPDATE_TASK(TASK TASK_TYPE);


  FUNCTION SEQ_NEXT_VAL RETURN NUMBER;



  PROCEDURE RUN_INTERNAL_WORKER(OWNER#    IN NUMBER,
                                TASK_NAME IN VARCHAR2,
                                JOB_NAME  IN VARCHAR2);

  PROCEDURE STOP_TASK(OWNER#    IN NUMBER,
                      TASK_NAME IN VARCHAR2);


  FUNCTION OWNER_NAME_TO_NUM(OWNER_NAME IN VARCHAR2) RETURN NUMBER;


  PROCEDURE DROP_ALL_TASKS(OWNER_NAME IN VARCHAR2);


  FUNCTION DEFAULT_PARALLELISM RETURN PLS_INTEGER;


  PROCEDURE ASSERT_TASK_EXISTS(OWNER# IN NUMBER,
                               TASK   IN VARCHAR2);


  PROCEDURE ASSERT_CHUNK_EXISTS(OWNER# IN NUMBER,
                                TASK   IN VARCHAR2,
                                CHUNK  IN NUMBER);
END;
/

