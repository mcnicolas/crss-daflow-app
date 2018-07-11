-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 7/11/18 10:46 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.169 (192.168.242.169)', LOCKGRANTED = '2018-07-11 10:46:47.737' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.6.0/schema/changelog-20180711.groovy::20180711-1::dmendoza
DROP VIEW IF EXISTS dataflow.VW_STL_DAILY_STATUS;

CREATE VIEW dataflow.VW_STL_DAILY_STATUS AS
   SELECT 
       q.id, q.run_id, q.job_process, q.status, q.details, 
       q.job_execution_id, q.job_exec_start, q.job_exec_end, 
       q.meter_process_type, q.group_id, q.trading_date 
   FROM dataflow.batch_job_queue q 
   WHERE q.run_id IN 
       (SELECT max(inner_q.run_id) FROM dataflow.batch_job_queue inner_q 
        WHERE inner_q.job_process in ('GEN_INPUT_WS_TA','CALC_TA')
        AND inner_q.trading_date is not null
        GROUP BY inner_q.trading_date, inner_q.group_id, 
           inner_q.meter_process_type);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('20180711-1', 'dmendoza', 'src/main/resources/liquibase/2.6.0/schema/changelog-20180711.groovy', NOW(), 37, '7:51cb87079d6b111b690acd5d79758d51', 'sql; sql', '', 'EXECUTED', NULL, NULL, '3.5.3', '1277208075');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

