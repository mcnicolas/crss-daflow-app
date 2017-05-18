package com.pemc.crss.dataflow.app.support;

public class AddtlCompensationQuery {

    public static final String ADDTL_COMP_EXEC_QUERY
            = "SELECT "
            + "    A.JOB_EXECUTION_ID, A.START_TIME, A.END_TIME, A.STATUS, A.EXIT_CODE, A.EXIT_MESSAGE, "
            + "    A.CREATE_TIME, A.LAST_UPDATED, A.VERSION, A.JOB_CONFIGURATION_LOCATION "
            + "FROM "
            + "    %PREFIX%JOB_EXECUTION A "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS C on A.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS D on A.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS E on A.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID "
            + "WHERE "
            + "    A.JOB_INSTANCE_ID = ? "
            + "    AND A.STATUS like ? "
            + "    AND (TO_CHAR(C.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and C.KEY_NAME = 'startDate') "
            + "    AND (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'endDate') "
            + "    AND (E.STRING_VAL like ? and E.KEY_NAME = 'acPricingCondition') "
            + "ORDER BY JOB_EXECUTION_ID desc";

    public static final String ADDTL_COMP_INTS_QUERY
            = "SELECT "
            + "    A.JOB_INSTANCE_ID, A.JOB_NAME "
            + "FROM "
            + "    %PREFIX%JOB_INSTANCE A "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS F on B.JOB_EXECUTION_ID = F.JOB_EXECUTION_ID "
            + "WHERE "
            + "    A.JOB_NAME like 'calculateAddtlComp%' "
            + "    and B.STATUS like ? "
            + "    and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi:ss') like ? and D.KEY_NAME = 'startDate') "
            + "    and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi:ss') like ? and E.KEY_NAME = 'endDate') "
            + "    and (F.STRING_VAL like ? and F.KEY_NAME = 'acPricingCondition') "
            + "ORDER BY JOB_INSTANCE_ID desc";

    public static final String ADDTL_COMP_INTS_COUNT_QUERY
            = "SELECT "
            + "    COUNT(*) "
            + "FROM "
            + "    %PREFIX%JOB_INSTANCE A "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS F on B.JOB_EXECUTION_ID = F.JOB_EXECUTION_ID "
            + "WHERE "
            + "    A.JOB_NAME like 'calculateAddtlComp%' "
            + "    and B.STATUS like ? "
            + "    and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') "
            + "    and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate') "
            + "    and (F.STRING_VAL like ? and F.KEY_NAME = 'acPricingCondition') "
            + "ORDER BY JOB_INSTANCE_ID desc";

    public static final String ADDTL_COMP_DISTINCT_QUERY
            = "SELECT "
            + "    DISTINCT ON (D.DATE_VAL, E.DATE_VAL, F.STRING_VAL) "
            + "    D.DATE_VAL as start_date, E.DATE_VAL as end_date, F.STRING_VAL as pricing_condition "
            + "FROM "
            + "    %PREFIX%JOB_INSTANCE A "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID "
            + "JOIN "
            + "    %PREFIX%JOB_EXECUTION_PARAMS F on B.JOB_EXECUTION_ID = F.JOB_EXECUTION_ID "
            + "WHERE "
            + "    A.JOB_NAME like 'calculateAddtlComp%' "
            + "    and B.STATUS like ? "
            + "    and D.KEY_NAME = 'startDate' "
            + "    and E.KEY_NAME = 'endDate' "
            + "    and F.KEY_NAME = 'acPricingCondition' "
            + "ORDER BY D.DATE_VAL DESC, E.DATE_VAL DESC";

    public static final String ADDTL_COMP_DISTINCT_COUNT_QUERY
            = "SELECT"
            + "    COUNT(*)"
            + "FROM ("
            +      ADDTL_COMP_DISTINCT_QUERY
            + ") as dist_addtl";


}
