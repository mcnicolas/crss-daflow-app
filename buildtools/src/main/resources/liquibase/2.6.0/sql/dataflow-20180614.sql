-- execute using postgres user

-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/adrian/developer/project-files/exist/crss-p2/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 6/18/18 7:52 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '172.18.0.1 (172.18.0.1)', LOCKGRANTED = '2018-06-18 07:52:04.052' WHERE ID = 1 AND LOCKED = FALSE;

DROP VIEW IF EXISTS dataflow.vw_stl_jobs;

CREATE VIEW dataflow.vw_stl_jobs AS
                  SELECT *,
  CASE
  WHEN inner_q.process_type in ('PRELIM', 'DAILY', 'FINAL') THEN inner_q.billing_period
  WHEN inner_q.process_type = 'ADJUSTED' THEN
    COALESCE((string_to_array(inner_q.job_name, '-'  ))[2], null)
  ELSE null
  END AS parent_id
FROM (
       SELECT
         je.job_execution_id as job_execution_id,
         ji.job_instance_id  as job_instance_id,
         ji.job_name         as job_name,
         je.status as status,
         je.start_time as job_exec_start_time,
         je.end_time as job_exec_end_time,
         COALESCE((select jep.string_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'processType' limit 1), 'DAILY') AS process_type,
         (select jep.date_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name in ('startDate', 'date') limit 1) AS start_date,
         (select jep.date_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'endDate' limit 1) AS end_date,
         (select
            CASE
            WHEN jep.string_val = ''
              THEN null
            ELSE jep.string_val
            END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'bp' limit 1) AS billing_period,
         coalesce((select
                     CASE
                     WHEN jep.string_val = ''
                       THEN 'ALL'
                     ELSE jep.string_val
                     END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name in ('regionGroup','rg') limit 1),
                  coalesce(( select string_val from dataflow.batch_job_execution_params jep2
                  where jep2.job_execution_id in
                        (select job_execution_id from dataflow.batch_job_execution je2
                        where je2.job_instance_id in
                              (select long_val from dataflow.batch_job_execution_params jep3
                              where jep3.key_name = 'parentJob'
                                    and jep3.job_execution_id = je.job_execution_id))
                        and jep2.key_name in ('regionGroup','rg')
                           ),'ALL')) AS region_group
       FROM dataflow.batch_job_instance ji
         INNER JOIN dataflow.batch_job_execution je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
       WHERE ji.job_name LIKE 'stlReady%' ) inner_q;

ALTER table dataflow.vw_stl_jobs OWNER TO crss_dataflow;

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

