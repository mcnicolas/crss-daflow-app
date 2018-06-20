databaseChangeLog {

    changeSet(id: '20180614-2', author: 'aviesca') {
        sql('DROP VIEW IF EXISTS dataflow.vw_stl_jobs;')
        sql('CREATE VIEW dataflow.vw_stl_jobs AS\n' +
                '  SELECT *,\n' +
                '    CASE\n' +
                '    WHEN inner_q.process_type in (\'PRELIM\', \'DAILY\', \'FINAL\') THEN inner_q.billing_period\n' +
                '    WHEN inner_q.process_type = \'ADJUSTED\' THEN\n' +
                '      COALESCE((string_to_array(inner_q.job_name, \'-\'  ))[2], null)\n' +
                '    ELSE null\n' +
                '    END AS parent_id\n' +
                '  FROM (\n' +
                '    SELECT\n' +
                '      je.job_execution_id as job_execution_id,\n' +
                '      ji.job_instance_id  as job_instance_id,\n' +
                '      ji.job_name         as job_name,\n' +
                '      je.status as status,\n' +
                '      je.start_time as job_exec_start_time,\n' +
                '      je.end_time as job_exec_end_time,\n' +
                '      COALESCE((select jep.string_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'processType\' limit 1), \'DAILY\') AS process_type,\n' +
                '      (select jep.date_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name in (\'startDate\', \'date\') limit 1) AS start_date,\n' +
                '      (select jep.date_val from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'endDate\' limit 1) AS end_date,\n' +
                '      (select\n' +
                '         CASE\n' +
                '         WHEN jep.string_val = \'\'\n' +
                '         THEN null\n' +
                '         ELSE jep.string_val\n' +
                '         END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'bp\' limit 1) AS billing_period,\n' +
                '      coalesce((select\n' +
                '         CASE\n' +
                '         WHEN jep.string_val = \'\'\n' +
                '         THEN \'ALL\'\n' +
                '         ELSE jep.string_val\n' +
                '         END from dataflow.batch_job_execution_params jep where jep.job_execution_id = je.job_execution_id and jep.key_name = \'regionGroup\' limit 1),\'ALL\') AS region_group\n' +
                '    FROM dataflow.batch_job_instance ji\n' +
                '    INNER JOIN dataflow.batch_job_execution je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID\n' +
                '    WHERE ji.job_name LIKE \'stlReady%\' ) inner_q;')
    }
}
