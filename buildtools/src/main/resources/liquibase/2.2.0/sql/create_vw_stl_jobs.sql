CREATE VIEW vw_stl_jobs AS
  SELECT
    je.job_execution_id as job_execution_id,
    ji.job_instance_id  as job_instance_id,
    ji.job_name         as job_name,
    je.status as status,
    je.start_time as job_exec_start_time,
    je.end_time as job_exec_end_time,
    COALESCE((select jep.string_val from batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'processType' limit 1), 'DAILY') AS process_type,
    (select jep.date_val from batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name in ('startDate', 'date') limit 1) AS start_date,
    (select jep.date_val from batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'endDate' limit 1) AS end_date,
    (select jep.long_val :: VARCHAR from batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = 'bp' limit 1) AS billing_period
  FROM batch_job_instance ji
    INNER JOIN batch_job_execution je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
  WHERE ji.job_name ILIKE '%stlReady%'
  ORDER BY start_date desc;
