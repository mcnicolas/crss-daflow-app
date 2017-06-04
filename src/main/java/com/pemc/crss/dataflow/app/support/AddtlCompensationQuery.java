package com.pemc.crss.dataflow.app.support;

public class AddtlCompensationQuery {

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
            + "    and (TO_CHAR(D.DATE_VAL, 'yyyy-MM-dd') like ? and D.KEY_NAME = 'startDate') "
            + "    and (TO_CHAR(E.DATE_VAL, 'yyyy-MM-dd') like ? and E.KEY_NAME = 'endDate') "
            + "    and (F.STRING_VAL like ? and F.KEY_NAME = 'acPC') "
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
            + "    and (D.KEY_NAME = 'startDate' and TO_CHAR(D.DATE_VAL, 'MM/DD/YYYY') like ?) "
            + "    and (E.KEY_NAME = 'endDate' and TO_CHAR(E.DATE_VAL, 'MM/DD/YYYY') like ?) "
            + "    and (F.KEY_NAME = 'acPC' and F.STRING_VAL like ?) "
            + "ORDER BY D.DATE_VAL DESC, E.DATE_VAL DESC";

    public static final String ADDTL_COMP_COMPLETE_FINALIZE_QUERY
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
            + "    A.JOB_NAME like 'calculateAddtlCompGmrVat%' "
            + "    and B.STATUS = 'COMPLETED' "
            + "    and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd') like ? and D.KEY_NAME = 'startDate') "
            + "    and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd') like ? and E.KEY_NAME = 'endDate') "
            + "    and (F.STRING_VAL like ? and F.KEY_NAME = 'acPC') "
            + "ORDER BY JOB_INSTANCE_ID desc";

    public static final String ADDTL_COMP_DISTINCT_COUNT_QUERY
            = "SELECT"
            + "    COUNT(*)"
            + "FROM ("
            +      ADDTL_COMP_DISTINCT_QUERY
            + ") as dist_addtl";


}
