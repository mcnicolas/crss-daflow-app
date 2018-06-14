-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 6/14/18 8:42 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.169 (192.168.242.169)', LOCKGRANTED = '2018-06-14 08:42:05.878' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1528936790096.groovy::1528936792820-1::dmendoza (generated)
ALTER TABLE dataflow.batch_job_queue ADD meter_process_type VARCHAR(255);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1528936792820-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1528936790096.groovy', NOW(), 32, '7:b40d3051659fb7e0f01d946c06e1f8a7', 'addColumn tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '8936926313');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

