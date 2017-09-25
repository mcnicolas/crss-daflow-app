-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 9/25/17 12:20 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-09-25 12:20:39.358' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1505357200479.groovy::1505357205385-1::dmendoza (generated)
CREATE INDEX IDX_BATCH_JOB_SKIP_LOGS ON dataflow.BATCH_JOB_SKIP_LOGS(JOB_EXECUTION_ID, PARENT_STEP_NAME);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1505357205385-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1505357200479.groovy', NOW(), 23, '7:3b39d31a9060a2bc10493a7d477af312', 'createIndex indexName=IDX_BATCH_JOB_SKIP_LOGS, tableName=BATCH_JOB_SKIP_LOGS', '', 'EXECUTED', NULL, NULL, '3.5.3', '6313239560');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

