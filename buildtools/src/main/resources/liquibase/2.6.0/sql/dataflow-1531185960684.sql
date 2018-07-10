-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 7/10/18 9:29 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.169 (192.168.242.169)', LOCKGRANTED = '2018-07-10 09:29:13.125' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.6.0/schema/changelog-1531185960684.groovy::1531185962837-1::dmendoza (generated)
ALTER TABLE dataflow.batch_job_queue ADD group_id VARCHAR(255);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1531185962837-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.6.0/schema/changelog-1531185960684.groovy', NOW(), 35, '7:bbae0e06e17a3df2ce293aa7ac58c004', 'addColumn tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '1186153541');

-- Changeset src/main/resources/liquibase/2.6.0/schema/changelog-1531185960684.groovy::1531185962837-2::dmendoza (generated)
ALTER TABLE dataflow.batch_job_queue ADD trading_date TIMESTAMP WITHOUT TIME ZONE;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1531185962837-2', 'dmendoza (generated)', 'src/main/resources/liquibase/2.6.0/schema/changelog-1531185960684.groovy', NOW(), 36, '7:14784861d619a16d2d03371ecaabd874', 'addColumn tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '1186153541');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

