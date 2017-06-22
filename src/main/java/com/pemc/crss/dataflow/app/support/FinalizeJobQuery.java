package com.pemc.crss.dataflow.app.support;

public class FinalizeJobQuery {

    public static final String FINALIZE_JOB_NAME_ADJ = "tasAsOutputReadyMonthlyAdjusted%";
    public static final String FINALIZE_JOB_NAME_FINAL = "tasAsOutputReadyF%";

    public static String countQuery() {
        return "SELECT count(*) FROM batch_job_instance ji JOIN batch_job_execution je ON ji.job_instance_id = je.job_instance_id JOIN batch_job_execution_params jep1 ON je.job_execution_id = jep1.job_execution_id JOIN batch_job_execution_params jep2 ON je.job_execution_id = jep2.job_execution_id JOIN batch_job_execution_params jep3 ON je.job_execution_id = jep3.job_execution_id WHERE ji.job_name like ? AND je.status = 'COMPLETED' AND (jep1.KEY_NAME = 'startDate' AND TO_CHAR(jep1.DATE_VAL, 'YYYY-MM-DD') LIKE ?) AND (jep2.KEY_NAME = 'endDate' AND TO_CHAR(jep2.DATE_VAL, 'YYYY-MM-DD') LIKE ?) AND (jep3.KEY_NAME = 'processType' AND jep3.STRING_VAL = ?)";
    }

}
