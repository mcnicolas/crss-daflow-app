-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 8/17/17 11:04 AM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-08-17 11:04:42.308' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy::1501669449638-1::dmendoza (generated)
CREATE TABLE dataflow.SETTLEMENT_JOB_LOCK (id BIGINT NOT NULL, created_datetime TIMESTAMP WITHOUT TIME ZONE, parent_job_id BIGINT NOT NULL, group_id VARCHAR(255) NOT NULL, start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, process_type VARCHAR(255) NOT NULL, stl_calculation_type VARCHAR(255) NOT NULL);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1501669449638-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy', NOW(), 19, '7:b72b6281e76fc477624bc14676f9fe86', 'createTable tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '2939082544');

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy::1501669449638-2::dmendoza (generated)
ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK ADD CONSTRAINT SETTLEMENT_JOB_LOCKPK PRIMARY KEY (id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1501669449638-2', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy', NOW(), 20, '7:67d2e074806e64e1a1f53c822c461ce0', 'addPrimaryKey constraintName=SETTLEMENT_JOB_LOCKPK, tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '2939082544');

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy::1501669449638-3::dmendoza (generated)
ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK ADD CONSTRAINT UK_STL_JOB_LOCK UNIQUE (group_id, process_type, stl_calculation_type);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1501669449638-3', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy', NOW(), 21, '7:21efbfb298fbb0d96b93c6d36021d43a', 'addUniqueConstraint constraintName=UK_STL_JOB_LOCK, tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '2939082544');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

