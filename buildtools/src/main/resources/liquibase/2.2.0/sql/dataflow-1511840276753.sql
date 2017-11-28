-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 11/28/17 11:39 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-11-28 11:39:08.272' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1511840276753.groovy::1511840279004-3::dmendoza (generated)
CREATE INDEX STEP_ID_IDX ON dataflow.batch_job_skip_logs(step_id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1511840279004-3', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1511840276753.groovy', NOW(), 24, '7:d410103993380e17d5cd227285d8da48', 'createIndex indexName=STEP_ID_IDX, tableName=batch_job_skip_logs', '', 'EXECUTED', NULL, NULL, '3.5.3', '1840348609');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

