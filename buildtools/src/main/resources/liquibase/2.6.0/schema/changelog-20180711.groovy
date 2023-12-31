databaseChangeLog {

    changeSet(id: '20180711-1', author: 'dmendoza') {
        sql('DROP VIEW IF EXISTS dataflow.VW_STL_DAILY_STATUS;')
        sql('CREATE VIEW dataflow.VW_STL_DAILY_STATUS AS\n'
            +   '   SELECT \n'
            +   '       q.id, q.run_id, q.job_process, q.status, q.details, \n'
            +   '       q.job_execution_id, q.job_exec_start, q.job_exec_end, \n'
            +   '       q.meter_process_type, q.group_id, q.region_group, q.trading_date \n'
            +   '   FROM dataflow.batch_job_queue q \n'
            +   '   WHERE q.run_id IN \n'
            +   '       (SELECT max(inner_q.run_id) FROM dataflow.batch_job_queue inner_q \n'
            +   '        WHERE inner_q.job_process in (\'GEN_INPUT_WS_TA\',\'CALC_TA\') \n'
            +   '        AND inner_q.trading_date is not null \n'
            +   '        AND inner_q.status != \'CANCELLED\' \n'
            +   '        GROUP BY inner_q.trading_date, inner_q.group_id, inner_q.region_group \n'
            +   '           inner_q.meter_process_type); ')
    }
}
