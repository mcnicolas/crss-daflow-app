databaseChangeLog {

    changeSet(id: '20181104-1', author: 'mnicolas') {
        createTable(tableName: 'map_billing_period_adjustment_no') {
            column(name: 'id', type: 'BIGINT') {
                constraints(nullable: false)
            }
            column(name: 'billing_period', type: 'VARCHAR(255)') {
                constraints(nullable: false)
            }
            column(name: 'adjustment_no', type: 'BIGINT') {
                constraints(nullable: false)
            }
            column(name: 'job_id', type: 'BIGINT') {
                constraints(nullable: false)
            }
        }
    }

    changeSet(id: '20181104-2', author: 'mnicolas') {
        sql('DROP VIEW IF EXISTS dataflow.vw_stl_jobs;')
        sql('CREATE VIEW dataflow.vw_stl_jobs AS\n' +
            '                  SELECT *,\n' +
            '  CASE\n' +
            '  WHEN inner_q.process_type in (\'PRELIM\', \'DAILY\', \'FINAL\') THEN inner_q.billing_period\n' +
            '  WHEN inner_q.process_type = \'ADJUSTED\' THEN\n' +
            '    inner_q.billing_period || inner_q.adjustment_no::TEXT\n' +
            '  ELSE null\n' +
            '  END AS parent_id\n' +
            'FROM (\n' +
            '  (SELECT\n' +
            '         je.job_execution_id as job_execution_id,\n' +
            '         ji.job_instance_id  as job_instance_id,\n' +
            '         ji.job_name         as job_name,\n' +
            '         je.status as status,\n' +
            '         je.start_time as job_exec_start_time,\n' +
            '         je.end_time as job_exec_end_time,\n' +
            '         COALESCE((select jep.string_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'processType\' limit 1), \'DAILY\') AS process_type,\n' +
            '         (select jep.date_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name in (\'startDate\', \'date\') limit 1) AS start_date,\n' +
            '         (select jep.date_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'endDate\' limit 1) AS end_date,\n' +
            '         (select\n' +
            '            CASE\n' +
            '            WHEN jep.string_val = \'\'\n' +
            '              THEN null\n' +
            '            ELSE jep.string_val\n' +
            '            END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'bp\' limit 1) AS billing_period,\n' +
            '         coalesce((select\n' +
            '                     CASE\n' +
            '                     WHEN jep.string_val = \'\'\n' +
            '                       THEN \'ALL\'\n' +
            '                     ELSE jep.string_val\n' +
            '                     END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name in (\'regionGroup\',\'rg\') limit 1),\n' +
            '                  coalesce(( select string_val from dataflow.batch_job_execution_params jep2\n' +
            '                  where jep2.job_execution_id in\n' +
            '                        (select job_execution_id from dataflow.batch_job_execution je2\n' +
            '                        where je2.job_instance_id in\n' +
            '                              (select long_val from dataflow.batch_job_execution_params jep3\n' +
            '                              where jep3.key_name = \'parentJob\'\n' +
            '                                    and jep3.job_execution_id = je.job_execution_id))\n' +
            '                        and jep2.key_name in (\'regionGroup\',\'rg\')\n' +
            '                           ),\'ALL\')) AS region_group\n' +
            '       FROM dataflow.batch_job_instance ji\n' +
            '         INNER JOIN dataflow.batch_job_execution je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID\n' +
            '       WHERE ji.job_name LIKE \'stlReady%\') old_q\n' +
            '         left join (select billing_period as adj_billing_period, adjustment_no from dataflow.map_billing_period_adjustment_no group by billing_period, adjustment_no) adj_no\n' +
            '           on old_q.billing_period = adj_no.adj_billing_period and old_q.process_type = \'ADJUSTED\') inner_q;')
    }
}
