package com.pemc.crss.dataflow.app.support;

public class StlCalculationQuery {
    public static final String CALC_JOB_NAME_ADJ = "calcSTL_AMTA-";
    public static final String CALC_JOB_NAME_FINAL = "calcSTL_AMTF-";
    public static final String CALC_JOB_NAME_PRELIM = "calcSTL_AMTP-";
    public static final String CALC_JOB_NAME_DAILY = "calcSTL_AMTD-";

    public static String executionQuery() {
        return "SELECT je.* FROM batch_job_instance ji JOIN batch_job_execution je ON ji.job_instance_id = je.job_instance_id JOIN batch_job_execution_params jep1 ON je.job_execution_id = jep1.job_execution_id JOIN batch_job_execution_params jep2 ON je.job_execution_id = jep2.job_execution_id JOIN batch_job_execution_params jep3 ON je.job_execution_id = jep3.job_execution_id WHERE ji.job_name like ? AND je.status = 'COMPLETED' AND (jep1.KEY_NAME = 'startDate' AND jep1.DATE_VAL BETWEEN TO_DATE(?, 'YYYY-MM-DD') AND TO_DATE(?, 'YYYY-MM-DD')) AND (jep2.KEY_NAME = 'endDate' AND jep2.DATE_VAL BETWEEN TO_DATE(?, 'YYYY-MM-DD') AND TO_DATE(?, 'YYYY-MM-DD')) AND (jep3.KEY_NAME = 'processType' AND jep3.STRING_VAL = ?) ORDER BY JE.START_TIME;";
    }
}
