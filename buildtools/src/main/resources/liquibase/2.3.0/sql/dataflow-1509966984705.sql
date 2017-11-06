-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 11/6/17 7:17 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-11-06 19:17:19.336' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1509966984705.groovy::1509966985983-2::dmendoza (generated)
ALTER TABLE dataflow.batch_job_queue ADD starting_date TIMESTAMP WITHOUT TIME ZONE;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1509966985983-2', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1509966984705.groovy', NOW(), 28, '7:956b176d8ae52e7578a53c25eaf2b376', 'addColumn tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '9967039706');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

