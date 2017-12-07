-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 12/7/17 1:42 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-12-07 13:42:47.863' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1512625182857.groovy::1512625185875-3::dmendoza (generated)
DROP TABLE dataflow.latest_adjustment_lock;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1512625185875-3', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1512625182857.groovy', NOW(), 30, '7:f748c5edeb56c5d688588c83ea98a228', 'dropTable tableName=latest_adjustment_lock', '', 'EXECUTED', NULL, NULL, '3.5.3', '2625368316');

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1512625182857.groovy::1512625185875-4::dmendoza (generated)
DROP TABLE dataflow.running_adjustment_lock;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1512625185875-4', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1512625182857.groovy', NOW(), 31, '7:fb43f11269b4fdb59afa65886bfa3039', 'dropTable tableName=running_adjustment_lock', '', 'EXECUTED', NULL, NULL, '3.5.3', '2625368316');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

