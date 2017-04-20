package com.pemc.crss.dataflow.app.support;

public class StlJobQuery {

    public static final String DEFAULT_TRADING_DATE_START = "01/01/1970";
    public static final String DEFAULT_TRADING_DATE_END = "12/31/2099";

    //TODO: remove % prefix once all stl ready job names in batch_job_instance start with 'stlReady'
    private static final String STL_READY_JOB_NAME = "%stlReady%";

    private static final String JOB_INSTANCE_SELECT = "SELECT JI.JOB_INSTANCE_ID, JI.JOB_NAME ";
    private static final String JOB_INSTANCE_COUNT = "SELECT COUNT(1) ";
    private static final String FILTER_PREDICATE_ALL =
    "FROM BATCH_JOB_INSTANCE JI "
        + "JOIN BATCH_JOB_EXECUTION JE ON JI.JOB_INSTANCE_ID = JE.JOB_INSTANCE_ID "
    + "WHERE JI.JOB_NAME ILIKE '"+ STL_READY_JOB_NAME +"' AND JE.STATUS = 'COMPLETED' ";

    private static final String FILTER_PREDICATE_DAILY =
    "FROM BATCH_JOB_INSTANCE JI "
        + "JOIN BATCH_JOB_EXECUTION JE ON JI.JOB_INSTANCE_ID = JE.JOB_INSTANCE_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_DATE ON JE.JOB_EXECUTION_ID = JP_DATE.JOB_EXECUTION_ID "
    + "WHERE JI.JOB_NAME ILIKE '"+ STL_READY_JOB_NAME +"' AND JE.STATUS = 'COMPLETED' "
        + "AND (JP_DATE.KEY_NAME = 'date' AND JP_DATE.DATE_VAL BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY')) ";

    private static final String FILTER_PREDICATE_MONTHLY =
    "FROM BATCH_JOB_INSTANCE JI "
        + "JOIN BATCH_JOB_EXECUTION JE ON JI.JOB_INSTANCE_ID = JE.JOB_INSTANCE_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_PROCESSTYPE ON JE.JOB_EXECUTION_ID = JP_PROCESSTYPE.JOB_EXECUTION_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_STARTDATE ON JE.JOB_EXECUTION_ID = JP_STARTDATE.JOB_EXECUTION_ID "
        + "JOIN BATCH_JOB_EXECUTION_PARAMS JP_ENDDATE ON JE.JOB_EXECUTION_ID = JP_ENDDATE.JOB_EXECUTION_ID "
    + "WHERE JI.JOB_NAME ILIKE '"+ STL_READY_JOB_NAME +"' AND JE.STATUS = 'COMPLETED' "
        + "AND (JP_PROCESSTYPE.KEY_NAME = 'processType' AND JP_PROCESSTYPE.STRING_VAL LIKE ?) "
        + "AND (JP_STARTDATE.KEY_NAME = 'startDate' AND TO_CHAR(JP_STARTDATE.DATE_VAL, 'MM/DD/YYYY') LIKE ?) "
        + "AND (JP_ENDDATE.KEY_NAME = 'endDate' AND TO_CHAR(JP_ENDDATE.DATE_VAL, 'MM/DD/YYYY') LIKE ?) ";

    private static final String JOB_INSTANCE_SORT = "ORDER BY JE.START_TIME DESC";

    public static String stlFilterAllCountQuery() {
        return JOB_INSTANCE_COUNT + FILTER_PREDICATE_ALL;
    }

    public static String stlFilterAllSelectQuery() {
        return JOB_INSTANCE_SELECT + FILTER_PREDICATE_ALL + JOB_INSTANCE_SORT;
    }

    public static String stlFilterDailyCountQuery() {
        return JOB_INSTANCE_COUNT + FILTER_PREDICATE_DAILY;
    }

    public static String stlFilterDailySelectQuery() {
        return JOB_INSTANCE_SELECT + FILTER_PREDICATE_DAILY + JOB_INSTANCE_SORT;
    }

    public static String stlFilterMonthlyCountQuery() {
        return JOB_INSTANCE_COUNT + FILTER_PREDICATE_MONTHLY;
    }

    public static String stlFilterMonthlySelectQuery() {
        return JOB_INSTANCE_SELECT + FILTER_PREDICATE_MONTHLY + JOB_INSTANCE_SORT;
    }
}
