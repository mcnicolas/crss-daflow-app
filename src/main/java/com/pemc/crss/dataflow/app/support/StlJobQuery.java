package com.pemc.crss.dataflow.app.support;

public class StlJobQuery {

    private static final String JOB_INSTANCE_SELECT = "SELECT JI.JOB_INSTANCE_ID, JI.JOB_NAME ";
    private static final String JOB_INSTANCE_COUNT = "SELECT COUNT(1) ";
    private static final String FILTER_PREDICATE =
    "FROM BATCH_JOB_INSTANCE JI "
        + "JOIN BATCH_JOB_EXECUTION JE ON JI.JOB_INSTANCE_ID = JE.JOB_INSTANCE_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_PROCESSTYPE ON JE.JOB_EXECUTION_ID = JP_PROCESSTYPE.JOB_EXECUTION_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_STARTDATE ON JE.JOB_EXECUTION_ID = JP_STARTDATE.JOB_EXECUTION_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_ENDDATE ON JE.JOB_EXECUTION_ID = JP_ENDDATE.JOB_EXECUTION_ID "
    + "WHERE JI.JOB_NAME LIKE 'processStlReady%' "
        + "AND (JP_PROCESSTYPE.KEY_NAME = 'processType' AND JP_PROCESSTYPE.STRING_VAL LIKE ?) "
        + "AND (JP_STARTDATE.KEY_NAME = 'startDate' AND TO_CHAR(JP_STARTDATE.DATE_VAL, 'yyyy-mm-dd') LIKE ?) "
        + "AND (JP_ENDDATE.KEY_NAME = 'endDate' AND TO_CHAR(JP_ENDDATE.DATE_VAL, 'yyyy-mm-dd') LIKE ?) ";

    private static final String JOB_INSTANCE_SORT = "ORDER BY JE.START_TIME DESC";

    public static String stlFilterCountQuery() {
        return JOB_INSTANCE_COUNT + FILTER_PREDICATE;
    }

    public static String stlFilterSelectQuery() {
        return JOB_INSTANCE_SELECT + FILTER_PREDICATE + JOB_INSTANCE_SORT;
    }
}
