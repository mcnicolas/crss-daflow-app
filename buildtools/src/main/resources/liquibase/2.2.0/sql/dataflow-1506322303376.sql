-- no corresponding changelog as the table involved in this script is a spring dataflow created table
CREATE INDEX IDX_BATCH_JOB_EXEC_PARAMS ON dataflow.BATCH_JOB_EXECUTION_PARAMS(JOB_EXECUTION_ID, KEY_NAME);
