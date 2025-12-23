create PACKAGE BODY dbms_parallel_execute_bublik AS

  PROCEDURE CREATE_CHUNKS_BY_ROWID_CHECK(TBL_OWN VARCHAR2, TBL_NM VARCHAR2)
  IS
    L_IOT          SYS.ALL_TABLES.IOT_TYPE%TYPE;
    L_CLUSTER_NAME SYS.ALL_TABLES.CLUSTER_NAME%TYPE;
  BEGIN
    SELECT  T.IOT_TYPE, T.CLUSTER_NAME
      INTO  L_IOT, L_CLUSTER_NAME
      FROM  SYS.ALL_TABLES T
      WHERE T.OWNER      = TBL_OWN
        AND T.TABLE_NAME = TBL_NM;

    IF (L_IOT IS NOT NULL OR L_CLUSTER_NAME IS NOT NULL) THEN
      RAISE INVALID_TABLE;
    END IF;
  EXCEPTION
    WHEN INVALID_TABLE THEN
      RAISE;
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(SQLERRM);
      RAISE INVALID_TABLE;
  END;



  PROCEDURE WAIT_JOBS(JOB_NAME_PREFIX VARCHAR2) IS
    L_NOT_DONE_JOBS   PLS_INTEGER;
    L_JOB_NAME_PREFIX DBMS_ID;
  BEGIN
    L_JOB_NAME_PREFIX := JOB_NAME_PREFIX||'%';
    LOOP
      SELECT COUNT(*) INTO L_NOT_DONE_JOBS
        FROM SYS.USER_SCHEDULER_JOBS
        WHERE JOB_NAME LIKE L_JOB_NAME_PREFIX
          AND STATE IN ('DISABLED', 'RETRY SCHEDULED', 'SCHEDULED', 'RUNNING');

      EXIT WHEN L_NOT_DONE_JOBS = 0;
      DBMS_LOCK.SLEEP(3);
    END LOOP;
  END;

  PROCEDURE ADM_ROLE_CHECK
  IS
    ROLE_CNT NUMBER;
  BEGIN
    ROLE_CNT := DBMS_PRIV_CAPTURE.SES_HAS_ROLE_PRIV('ADM_PARALLEL_EXECUTE_TASK');

    IF (ROLE_CNT <= 0) THEN
      RAISE MISSING_ROLE;
    END IF;
  END;

  FUNCTION GENERATE_TASK_NAME(PREFIX IN VARCHAR2 DEFAULT 'TASK$_')
  RETURN VARCHAR2
  IS
  BEGIN
    RETURN dbms_parallel_execute_i_b.GENERATE_TASK_NAME(PREFIX);
  END;

  PROCEDURE CREATE_TASK(TASK_NAME  IN VARCHAR2,
                        COMMENT    IN VARCHAR2 DEFAULT NULL)
  IS
  BEGIN
    dbms_parallel_execute_i_b.CREATE_TASK(USERENV('SCHEMAID'), TASK_NAME,
                                               COMMENT);
  END;

  PROCEDURE DROP_TASK(TASK_NAME IN VARCHAR2)
  IS
  BEGIN
    dbms_parallel_execute_i_b.DROP_TASK(USERENV('SCHEMAID'), TASK_NAME);
  END;

  PROCEDURE CREATE_CHUNKS_BY_ROWID(TASK_NAME   IN VARCHAR2,
                                   TABLE_OWNER IN VARCHAR2,
                                   TABLE_NAME  IN VARCHAR2,
                                   BY_ROW      IN BOOLEAN,
                                   CHUNK_SIZE  IN NUMBER)
  IS
  BEGIN

    CREATE_CHUNKS_BY_ROWID_CHECK(TABLE_OWNER, TABLE_NAME);


    IF (BY_ROW) THEN
      dbms_parallel_execute_i_b.CREATE_CHUNKS_BY_ROWID(
        USERENV('SCHEMAID'), TASK_NAME, TABLE_OWNER, TABLE_NAME,
        NUM_ROW=>CHUNK_SIZE);
    ELSE
      dbms_parallel_execute_i_b.CREATE_CHUNKS_BY_ROWID(
        USERENV('SCHEMAID'), TASK_NAME, TABLE_OWNER, TABLE_NAME,
        NUM_BLOCK=>CHUNK_SIZE);
    END IF;
  END;

  PROCEDURE CREATE_CHUNKS_BY_NUMBER_COL(TASK_NAME    IN VARCHAR2,
                                        TABLE_OWNER  IN VARCHAR2,
                                        TABLE_NAME   IN VARCHAR2,
                                        TABLE_COLUMN IN VARCHAR2,
                                        CHUNK_SIZE   IN NUMBER)
  IS
  BEGIN

    dbms_parallel_execute_i_b.CREATE_CHUNKS_BY_NUMBER_COL(
      USERENV('SCHEMAID'), TASK_NAME, TABLE_OWNER, TABLE_NAME,
      TABLE_COLUMN, CHUNK_SIZE);
  END;

  PROCEDURE CREATE_CHUNKS_BY_SQL(TASK_NAME IN VARCHAR2,
                                 SQL_STMT  IN CLOB,
                                 BY_ROWID  IN BOOLEAN)
  IS
  BEGIN
    dbms_parallel_execute_i_b.CREATE_CHUNKS_BY_SQL(
      USERENV('SCHEMAID'), TASK_NAME, SQL_STMT, BY_ROWID);
  END;

  PROCEDURE DROP_CHUNKS(TASK_NAME IN VARCHAR2)
  IS
  BEGIN
    dbms_parallel_execute_i_b.DROP_CHUNKS(USERENV('SCHEMAID'),
                                               TASK_NAME);
  END;

  PROCEDURE GET_ROWID_CHUNK(TASK_NAME   IN  VARCHAR2,
                            CHUNK_ID    OUT NUMBER,
                            START_ROWID OUT ROWID,
                            END_ROWID   OUT ROWID,
                            ANY_ROWS    OUT BOOLEAN)
  IS
    L_START_ID NUMBER;
    L_END_ID   NUMBER;
  BEGIN
    ANY_ROWS := dbms_parallel_execute_i_b.GET_RANGE(
                  USERENV('SCHEMAID'), TASK_NAME, CHUNK_ID,
                  START_ROWID, END_ROWID, L_START_ID, L_END_ID);
  END;

  PROCEDURE GET_NUMBER_COL_CHUNK(TASK_NAME IN  VARCHAR2,
                                 CHUNK_ID  OUT NUMBER,
                                 START_ID  OUT NUMBER,
                                 END_ID    OUT NUMBER,
                                 ANY_ROWS  OUT BOOLEAN)
  IS
    L_START_ROWID ROWID;
    L_END_ROWID   ROWID;
  BEGIN
    ANY_ROWS := dbms_parallel_execute_i_b.GET_RANGE(
                  USERENV('SCHEMAID'), TASK_NAME, CHUNK_ID,
                  L_START_ROWID, L_END_ROWID, START_ID, END_ID);
  END;






  PROCEDURE SET_CHUNK_STATUS(TASK_NAME IN VARCHAR2,
                             CHUNK_ID  IN NUMBER,
                             STATUS    IN NUMBER,
                             ERR_NUM   IN NUMBER   DEFAULT NULL,
                             ERR_MSG   IN VARCHAR2 DEFAULT NULL)
  IS
  BEGIN
    dbms_parallel_execute_i_b.SET_CHUNK_STATUS(
      USERENV('SCHEMAID'), TASK_NAME, CHUNK_ID, STATUS, ERR_NUM, ERR_MSG);
  END;






  PROCEDURE PURGE_PROCESSED_CHUNKS(TASK_NAME IN VARCHAR2)
  IS
  BEGIN
    dbms_parallel_execute_i_b.PURGE_PROCESSED_CHUNKS(
      USERENV('SCHEMAID'), TASK_NAME);
  END;





  FUNCTION TASK_STATUS(TASK_NAME IN VARCHAR2) RETURN NUMBER
  IS
  BEGIN
    RETURN dbms_parallel_execute_i_b.TASK_STATUS(
             USERENV('SCHEMAID'), TASK_NAME);
  END;






  PROCEDURE RUN_INTERNAL_WORKER(TASK_NAME IN VARCHAR2,
                                JOB_NAME  IN VARCHAR2)
  IS
  BEGIN
    dbms_parallel_execute_i_b.RUN_INTERNAL_WORKER(
      USERENV('SCHEMAID'), TASK_NAME, JOB_NAME);
  END;









  PROCEDURE RUN_TASK_INTERNAL(
    TASK_NAME                  IN VARCHAR2,
    SQL_STMT                   IN CLOB,
    LANGUAGE_FLAG              IN NUMBER,
    EDITION                    IN VARCHAR2 DEFAULT NULL,
    APPLY_CROSSEDITION_TRIGGER IN VARCHAR2 DEFAULT NULL,
    FIRE_APPLY_TRIGGER         IN BOOLEAN  DEFAULT TRUE,
    PARALLEL_LEVEL             IN NUMBER   DEFAULT 0,
    JOB_CLASS                  IN VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS')
  IS
    L_PARALLEL_LEVEL     NUMBER;
    L_JOB_PRE            DBMS_ID;
    L_JOB_NAME           DBMS_ID;
    L_CNT                NUMBER;
    J NUMBER;
    L_TASK_ROW           dbms_parallel_execute_i_b.TASK_TYPE;
  BEGIN


    L_TASK_ROW := dbms_parallel_execute_i_b.READ_TASK(USERENV('SCHEMAID'),
                                                           TASK_NAME);
    L_TASK_ROW.SQL_STMT                   := SQL_STMT;
    L_TASK_ROW.LANGUAGE_FLAG              := LANGUAGE_FLAG;
    L_TASK_ROW.APPLY_CROSSEDITION_TRIGGER := APPLY_CROSSEDITION_TRIGGER;
    L_TASK_ROW.PARALLEL_LEVEL             := PARALLEL_LEVEL;
    L_TASK_ROW.JOB_CLASS                  := JOB_CLASS;
    IF (EDITION IS NULL) THEN
      L_TASK_ROW.EDITION := SYS_CONTEXT('userenv', 'current_edition_name');
    ELSE
      L_TASK_ROW.EDITION := EDITION;
    END IF;
    IF (FIRE_APPLY_TRIGGER) THEN L_TASK_ROW.FIRE_APPLY_TRIGGER := 'TRUE';
    ELSE                         L_TASK_ROW.FIRE_APPLY_TRIGGER := 'FALSE';
    END IF;


    L_TASK_ROW.STOP_FLAG := 0;

    dbms_parallel_execute_i_b.UPDATE_TASK(L_TASK_ROW);
    COMMIT;





    IF (PARALLEL_LEVEL <= 1) THEN
      RUN_INTERNAL_WORKER(TASK_NAME, NULL);
      RETURN;
    END IF;


















    L_JOB_PRE             := DBMS_SCHEDULER.GENERATE_JOB_NAME('TASK$_');
    L_TASK_ROW.JOB_PREFIX := L_JOB_PRE;
    dbms_parallel_execute_i_b.UPDATE_TASK(L_TASK_ROW);
    COMMIT;


    IF (PARALLEL_LEVEL IS NULL) THEN
      L_PARALLEL_LEVEL := dbms_parallel_execute_i_b.DEFAULT_PARALLELISM();
    ELSE
      L_PARALLEL_LEVEL := PARALLEL_LEVEL;
    END IF;

    FOR J IN 1..L_PARALLEL_LEVEL LOOP

      L_JOB_NAME := L_JOB_PRE || '_' || TO_CHAR(J);


      DBMS_SCHEDULER.CREATE_JOB(
        JOB_NAME            => L_JOB_NAME,
        JOB_TYPE            => 'STORED_PROCEDURE',
        JOB_ACTION          => 'DBMS_PARALLEL_EXECUTE_BUBLIK.RUN_INTERNAL_WORKER',
        NUMBER_OF_ARGUMENTS => 2,
        JOB_CLASS           => JOB_CLASS);


      DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(
        JOB_NAME          => L_JOB_NAME,
        ARGUMENT_POSITION => 1,
        ARGUMENT_VALUE    => TASK_NAME);

      DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(
        JOB_NAME          => L_JOB_NAME,
        ARGUMENT_POSITION => 2,
        ARGUMENT_VALUE    => L_JOB_NAME);



      DBMS_SCHEDULER.ENABLE(L_JOB_NAME);

    END LOOP;










    WAIT_JOBS(L_JOB_PRE);
  END;







  PROCEDURE RUN_TASK(
    TASK_NAME                  IN VARCHAR2,
    SQL_STMT                   IN CLOB,
    LANGUAGE_FLAG              IN NUMBER,
    EDITION                    IN VARCHAR2 DEFAULT NULL,
    APPLY_CROSSEDITION_TRIGGER IN VARCHAR2 DEFAULT NULL,
    FIRE_APPLY_TRIGGER         IN BOOLEAN  DEFAULT TRUE,
    PARALLEL_LEVEL             IN NUMBER   DEFAULT 0,
    JOB_CLASS                  IN VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS')
  IS
  BEGIN

    IF (TASK_STATUS(TASK_NAME) = NO_CHUNKS) THEN
      RETURN;
    ELSIF (TASK_STATUS(TASK_NAME) != CHUNKED) THEN
      RAISE INVALID_STATE_FOR_RUN;
    END IF;


    RUN_TASK_INTERNAL(TASK_NAME,
                      SQL_STMT,
                      LANGUAGE_FLAG,
                      EDITION,
                      APPLY_CROSSEDITION_TRIGGER,
                      FIRE_APPLY_TRIGGER,
                      PARALLEL_LEVEL,
                      JOB_CLASS);
  END;











  PROCEDURE RESUME_TASK(
    TASK_NAME                  IN VARCHAR2,
    SQL_STMT                   IN CLOB,
    LANGUAGE_FLAG              IN NUMBER,
    EDITION                    IN VARCHAR2 DEFAULT NULL,
    APPLY_CROSSEDITION_TRIGGER IN VARCHAR2 DEFAULT NULL,
    FIRE_APPLY_TRIGGER         IN BOOLEAN  DEFAULT TRUE,
    PARALLEL_LEVEL             IN NUMBER   DEFAULT 0,
    JOB_CLASS                  IN VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS',
    FORCE                      IN BOOLEAN  DEFAULT FALSE)
  IS
    L_STATUS    NUMBER;
  BEGIN


    L_STATUS := TASK_STATUS(TASK_NAME);
    IF ((L_STATUS = FINISHED_WITH_ERROR OR
         L_STATUS = CRASHED             OR
         (FORCE AND (L_STATUS = PROCESSING)))
        != TRUE)
    THEN
      RAISE INVALID_STATE_FOR_RESUME;
    END IF;


    dbms_parallel_execute_i_b.UNASSIGN_CHUNKS(USERENV('SCHEMAID'),
                                                   TASK_NAME);


    RUN_TASK_INTERNAL(TASK_NAME,
                      SQL_STMT,
                      LANGUAGE_FLAG,
                      EDITION,
                      APPLY_CROSSEDITION_TRIGGER,
                      FIRE_APPLY_TRIGGER,
                      PARALLEL_LEVEL,
                      JOB_CLASS);
  END;






  PROCEDURE RESUME_TASK(TASK_NAME IN VARCHAR2,
                        FORCE     IN BOOLEAN DEFAULT FALSE)
  IS
    L_TASK_ROW dbms_parallel_execute_i_b.TASK_TYPE;
    L_FIRE_TRG BOOLEAN;
  BEGIN
    L_TASK_ROW := dbms_parallel_execute_i_b.READ_TASK(USERENV('SCHEMAID'),
                                                           TASK_NAME);

    IF (L_TASK_ROW.FIRE_APPLY_TRIGGER = 'TRUE') THEN L_FIRE_TRG := TRUE;
    ELSE                                             L_FIRE_TRG := FALSE;
    END IF;

    RESUME_TASK(TASK_NAME,
                L_TASK_ROW.SQL_STMT,
                L_TASK_ROW.LANGUAGE_FLAG,
                L_TASK_ROW.EDITION,
                L_TASK_ROW.APPLY_CROSSEDITION_TRIGGER,
                L_FIRE_TRG,
                L_TASK_ROW.PARALLEL_LEVEL,
                L_TASK_ROW.JOB_CLASS,
                FORCE);
  END;





  PROCEDURE STOP_TASK(TASK_NAME IN VARCHAR2)
  IS
  BEGIN
    dbms_parallel_execute_i_b.STOP_TASK(USERENV('SCHEMAID'), TASK_NAME);
  END;




  PROCEDURE ADM_DROP_TASK(TASK_OWNER IN VARCHAR2,
                          TASK_NAME  IN VARCHAR2)
  IS
    OWNER# NUMBER;
  BEGIN
    ADM_ROLE_CHECK();
    OWNER# := dbms_parallel_execute_i_b.OWNER_NAME_TO_NUM(TASK_OWNER);
    dbms_parallel_execute_i_b.DROP_TASK(OWNER#, TASK_NAME);
  END;

  PROCEDURE ADM_DROP_CHUNKS(TASK_OWNER IN VARCHAR2,
                            TASK_NAME  IN VARCHAR2)
  IS
    OWNER# NUMBER;
  BEGIN
    ADM_ROLE_CHECK();
    OWNER# := dbms_parallel_execute_i_b.OWNER_NAME_TO_NUM(TASK_OWNER);
    dbms_parallel_execute_i_b.DROP_CHUNKS(OWNER#, TASK_NAME);
  END;

  FUNCTION ADM_TASK_STATUS(TASK_OWNER IN VARCHAR2,
                           TASK_NAME  IN VARCHAR2) RETURN NUMBER
  IS
    OWNER# NUMBER;
  BEGIN
    ADM_ROLE_CHECK();
    OWNER# := dbms_parallel_execute_i_b.OWNER_NAME_TO_NUM(TASK_OWNER);
    RETURN dbms_parallel_execute_i_b.TASK_STATUS(OWNER#, TASK_NAME);
  END;

  PROCEDURE ADM_STOP_TASK(TASK_OWNER IN VARCHAR2,
                          TASK_NAME  IN VARCHAR2)
  IS
    OWNER# NUMBER;
  BEGIN
    ADM_ROLE_CHECK();
    OWNER# := dbms_parallel_execute_i_b.OWNER_NAME_TO_NUM(TASK_OWNER);
    dbms_parallel_execute_i_b.STOP_TASK(OWNER#, TASK_NAME);
  END;
END;
/