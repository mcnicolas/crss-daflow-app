-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 6/21/18 2:17 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.169 (192.168.242.169)', LOCKGRANTED = '2018-06-21 14:17:25.069' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.6.0/schema/changelog-1529561667951.groovy::1529561672897-1::dmendoza (generated)
ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK ADD region_group VARCHAR(255);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1529561672897-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.6.0/schema/changelog-1529561667951.groovy', NOW(), 35, '7:9ea0db5b7bb0a3b3a7f5165311847311', 'addColumn tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '9561845862');

-- Changeset src/main/resources/liquibase/2.6.0/schema/changelog-1529561667951.groovy::1529561672897-2::dmendoza (generated)
ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK DROP CONSTRAINT UK_STL_JOB_LOCK;

ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK ADD CONSTRAINT UK_STL_JOB_LOCK UNIQUE (group_id, process_type, stl_calculation_type, region_group);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1529561672897-2', 'dmendoza (generated)', 'src/main/resources/liquibase/2.6.0/schema/changelog-1529561667951.groovy', NOW(), 36, '7:1fec038ccc8bd98c473d8a4d2257d6eb', 'dropUniqueConstraint constraintName=UK_STL_JOB_LOCK, tableName=SETTLEMENT_JOB_LOCK; addUniqueConstraint constraintName=UK_STL_JOB_LOCK, tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '9561845862');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

