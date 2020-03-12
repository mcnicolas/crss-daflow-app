ALTER table dataflow.map_billing_period_adjustment_no add COLUMN region_group VARCHAR(255);


update dataflow.map_billing_period_adjustment_no A set region_group = x.region_group
from
  (select adj_no.job_id, params.string_val as region_group from dataflow.map_billing_period_adjustment_no adj_no
join dataflow.batch_job_execution exe on adj_no.job_id = exe.job_instance_id
join dataflow.batch_job_execution_params params on params.job_execution_id = exe.job_execution_id
and params.key_name in ('regionGroup','rg')) x
where A.job_id = x.job_id
;


DROP VIEW IF EXISTS dataflow.vw_stl_jobs;

CREATE VIEW dataflow.vw_stl_jobs AS
  SELECT *,
    CASE
    WHEN inner_q.process_type in ('PRELIM', 'DAILY', 'FINAL') THEN inner_q.billing_period
    WHEN inner_q.process_type = 'ADJUSTED' THEN
      inner_q.billing_period || inner_q.adjustment_no::TEXT
    ELSE null
    END AS parent_id
  FROM (
           (SELECT
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
            WHERE ji.job_name LIKE 'stlReady%') old_q
           left join (select billing_period as adj_billing_period, adjustment_no, region_group as rg from dataflow.map_billing_period_adjustment_no group by billing_period, adjustment_no, region_group) adj_no
             on old_q.billing_period = adj_no.adj_billing_period and old_q.region_group = adj_no.rg and old_q.process_type = 'ADJUSTED') inner_q;

ALTER table dataflow.vw_stl_jobs OWNER TO crss_dataflow;
grant all on all tables in schema dataflow to crss_settlement;

