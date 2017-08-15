-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 8/2/17 6:30 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-08-02 18:30:18.474' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy::1501669449638-1::dmendoza (generated)
CREATE TABLE dataflow.SETTLEMENT_JOB_LOCK (id BIGINT NOT NULL, created_datetime TIMESTAMP WITHOUT TIME ZONE, end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, group_id VARCHAR(255) NOT NULL, locked BOOLEAN NOT NULL, parent_job_id BIGINT NOT NULL, process_type VARCHAR(255), start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1501669449638-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy', NOW(), 19, '7:dd4514c006e98f0f32e3575c82d7ab75', 'createTable tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '1669818661');

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy::1501669449638-2::dmendoza (generated)
ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK ADD CONSTRAINT SETTLEMENT_JOB_LOCKPK PRIMARY KEY (id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1501669449638-2', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy', NOW(), 20, '7:67d2e074806e64e1a1f53c822c461ce0', 'addPrimaryKey constraintName=SETTLEMENT_JOB_LOCKPK, tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '1669818661');

-- Changeset src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy::1501669449638-3::dmendoza (generated)
ALTER TABLE dataflow.SETTLEMENT_JOB_LOCK ADD CONSTRAINT UK_STL_JOB_LOCK UNIQUE (parent_job_id, group_id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1501669449638-3', 'dmendoza (generated)', 'src/main/resources/liquibase/2.2.0/schema/changelog-1501669448827.groovy', NOW(), 21, '7:9575bdb1eb43945dc1ebf3ae1d102edc', 'addUniqueConstraint constraintName=UK_STL_JOB_LOCK, tableName=SETTLEMENT_JOB_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '1669818661');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

