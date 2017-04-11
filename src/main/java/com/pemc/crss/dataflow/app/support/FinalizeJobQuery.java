package com.pemc.crss.dataflow.app.support;

public class FinalizeJobQuery {

    public static final String FINALIZE_JOB_NAME_ADJ = "tasAsOutputReadyMonthlyAdjusted%";
    public static final String FINALIZE_JOB_NAME_FINAL = "tasAsOutputReadyMonthlyFinal%";

    public static String countQuery() {
        return "SELECT count(*) FROM batch_job_instance ji JOIN batch_job_execution je ON ji.job_instance_id = je.job_instance_id JOIN batch_job_execution_params jep ON je.job_execution_id = jep.batch_job_execution_params WHERE ji.job_name like ? AND je.status = 'COMPLETED' AND (jep.KEY_NAME = 'startDate' AND TO_CHAR(jep.DATE_VAL, 'MM/DD/YYYY') LIKE ?) AND (jep.KEY_NAME = 'endDate' AND TO_CHAR(jep.DATE_VAL, 'MM/DD/YYYY') LIKE ?) AND (jep.KEY_NAME = 'processType' AND jep.STRING_VAL = ?) ORDER BY JE.START_TIME DESC";
    }

}
