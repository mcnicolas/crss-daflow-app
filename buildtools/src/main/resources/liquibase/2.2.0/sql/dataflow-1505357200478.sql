-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 9/14/17 10:54 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-09-14 10:54:24.088' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1505357200478.groovy::1505357205384-1::dmendoza (generated)
ALTER TABLE dataflow.batch_job_addtl_params ALTER COLUMN string_val TYPE VARCHAR(20000) USING (string_val::VARCHAR(20000));

-- additional alter column scripts for spring batch generated tables
ALTER TABLE dataflow.batch_job_execution_params ALTER COLUMN string_val TYPE varchar(20000);

ALTER TABLE dataflow.batch_job_execution_context ALTER COLUMN short_context TYPE varchar(20000);

ALTER TABLE dataflow.batch_step_execution_context ALTER COLUMN short_context TYPE varchar(20000);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1505357205384-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1505357200478.groovy', NOW(), 22, '7:1dbdb418bee43f6b5ce0f0b5498c8c8d', 'modifyDataType columnName=string_val, tableName=batch_job_addtl_params', '', 'EXECUTED', NULL, NULL, '3.5.3', '5357664344');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

