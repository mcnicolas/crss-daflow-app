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
         END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'bp' limit 1) AS billing_period
    FROM dataflow.batch_job_instance ji
    INNER JOIN dataflow.batch_job_execution je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
    WHERE ji.job_name LIKE 'stlReady%' ) inner_q;
