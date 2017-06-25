-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /Users/jdimayuga/workspace/crss/phase2/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 6/25/17 4:05 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Create Database Lock Table
CREATE TABLE dataflow.databasechangeloglock (ID INT NOT NULL, LOCKED BOOLEAN NOT NULL, LOCKGRANTED TIMESTAMP WITHOUT TIME ZONE, LOCKEDBY VARCHAR(255), CONSTRAINT PK_DATABASECHANGELOGLOCK PRIMARY KEY (ID));

-- Initialize Database Lock Table
DELETE FROM dataflow.databasechangeloglock;

INSERT INTO dataflow.databasechangeloglock (ID, LOCKED) VALUES (1, FALSE);

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.1.21 (192.168.1.21)', LOCKGRANTED = '2017-06-25 16:05:37.121' WHERE ID = 1 AND LOCKED = FALSE;

-- Create Database Change Log Table
CREATE TABLE dataflow.databasechangelog (ID VARCHAR(255) NOT NULL, AUTHOR VARCHAR(255) NOT NULL, FILENAME VARCHAR(255) NOT NULL, DATEEXECUTED TIMESTAMP WITHOUT TIME ZONE NOT NULL, ORDEREXECUTED INT NOT NULL, EXECTYPE VARCHAR(10) NOT NULL, MD5SUM VARCHAR(35), DESCRIPTION VARCHAR(255), COMMENTS VARCHAR(255), TAG VARCHAR(255), LIQUIBASE VARCHAR(20), CONTEXTS VARCHAR(255), LABELS VARCHAR(255), DEPLOYMENT_ID VARCHAR(10));

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-1::jdimayuga (generated)
CREATE SEQUENCE dataflow.batch_job_skip_logs_seq;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-1', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 1, '7:4bcbbc320eb14a62967eead50023540e', 'createSequence sequenceName=batch_job_skip_logs_seq', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-2::jdimayuga (generated)
CREATE SEQUENCE dataflow.hibernate_sequence;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-2', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 2, '7:afe897bea20bc08d1681087c94d0967d', 'createSequence sequenceName=hibernate_sequence', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-3::jdimayuga (generated)
CREATE TABLE dataflow.LATEST_ADJUSTMENT_LOCK (id BIGSERIAL NOT NULL, end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, group_id BIGINT NOT NULL, locked BOOLEAN, parent_job_id BIGINT NOT NULL, start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, CONSTRAINT LATEST_ADJUSTMENT_LOCKPK PRIMARY KEY (id));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-3', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 3, '7:75678e078512b2886b2aa5698211d67c', 'createTable tableName=LATEST_ADJUSTMENT_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-4::jdimayuga (generated)
CREATE TABLE dataflow.RUNNING_ADJUSTMENT_LOCK (id BIGSERIAL NOT NULL, end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, group_id BIGINT NOT NULL, locked BOOLEAN, parent_job_id BIGINT NOT NULL, start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, CONSTRAINT RUNNING_ADJUSTMENT_LOCKPK PRIMARY KEY (id));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-4', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 4, '7:7b293d22b3850afe4426a27ab7be500e', 'createTable tableName=RUNNING_ADJUSTMENT_LOCK', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-5::jdimayuga (generated)
CREATE TABLE dataflow.addtl_comp_params (id BIGSERIAL NOT NULL, approved_rate numeric(19, 2) NOT NULL, BILLING_END_DATE TIMESTAMP WITHOUT TIME ZONE NOT NULL, billing_id VARCHAR(255) NOT NULL, BILLING_START_DATE TIMESTAMP WITHOUT TIME ZONE NOT NULL, group_id VARCHAR(255) NOT NULL, job_id BIGINT, mtn VARCHAR(255) NOT NULL, pricing_condition VARCHAR(255) NOT NULL, status VARCHAR(255), CONSTRAINT addtl_comp_paramsPK PRIMARY KEY (id));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-5', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 5, '7:3303a7f0f6feb8c637baf7d3fd833a91', 'createTable tableName=addtl_comp_params', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-6::jdimayuga (generated)
CREATE TABLE dataflow.batch_job_addtl_params (id BIGSERIAL NOT NULL, created_datetime TIMESTAMP WITHOUT TIME ZONE, date_val TIMESTAMP WITHOUT TIME ZONE, double_val FLOAT8, key_name VARCHAR(255) NOT NULL, long_val BIGINT, run_id BIGINT NOT NULL, string_val VARCHAR(255), type_cd VARCHAR(255), CONSTRAINT batch_job_addtl_paramsPK PRIMARY KEY (id));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-6', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 6, '7:80b2d868b008218416450034971fa62c', 'createTable tableName=batch_job_addtl_params', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-7::jdimayuga (generated)
CREATE TABLE dataflow.batch_job_adj_run (id BIGSERIAL NOT NULL, created_datetime TIMESTAMP WITHOUT TIME ZONE, ADDTL_COMP CHAR(1) NOT NULL, BILLING_PERIOD_END TIMESTAMP WITHOUT TIME ZONE NOT NULL, BILLING_PERIOD_START TIMESTAMP WITHOUT TIME ZONE NOT NULL, group_id VARCHAR(255) NOT NULL, job_id VARCHAR(255) NOT NULL, METER_PROCESS_TYPE VARCHAR(50) NOT NULL, OUTPUT_READY CHAR(1) NOT NULL, OUTPUT_READY_DATETIME TIMESTAMP WITHOUT TIME ZONE, CONSTRAINT batch_job_adj_runPK PRIMARY KEY (id));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-7', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 7, '7:4695a4b365ed9af3e967a39204753b00', 'createTable tableName=batch_job_adj_run', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-8::jdimayuga (generated)
CREATE TABLE dataflow.batch_job_retry_attempt (job_execution_id BIGINT NOT NULL, retry_attempt INT);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-8', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 8, '7:f4b52097194d3bbde8c39b9e320d01b3', 'createTable tableName=batch_job_retry_attempt', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-9::jdimayuga (generated)
CREATE TABLE dataflow.batch_job_run_lock (id BIGSERIAL NOT NULL, created_datetime TIMESTAMP WITHOUT TIME ZONE, job_name VARCHAR(255), locked BOOLEAN, locked_date TIMESTAMP WITHOUT TIME ZONE, CONSTRAINT batch_job_run_lockPK PRIMARY KEY (id));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-9', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 9, '7:c28dff8b1c507d4a6170773313acc658', 'createTable tableName=batch_job_run_lock', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-10::jdimayuga (generated)
CREATE TABLE dataflow.batch_job_skip_logs (id BIGINT NOT NULL, details VARCHAR(255), error VARCHAR(255), error_code INT, step_id BIGINT, step_name VARCHAR(255));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-10', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 10, '7:87cc8262ff3e3f24ed6e121b74e7811a', 'createTable tableName=batch_job_skip_logs', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-11::jdimayuga (generated)
ALTER TABLE dataflow.batch_job_retry_attempt ADD CONSTRAINT batch_job_retry_attemptPK PRIMARY KEY (job_execution_id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-11', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 11, '7:3f9afe5353782a8eb45e7487757b19f8', 'addPrimaryKey constraintName=batch_job_retry_attemptPK, tableName=batch_job_retry_attempt', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-12::jdimayuga (generated)
ALTER TABLE dataflow.batch_job_skip_logs ADD CONSTRAINT batch_job_skip_logsPK PRIMARY KEY (id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-12', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 12, '7:2f862bd5df305b08881105f22f2d59fb', 'addPrimaryKey constraintName=batch_job_skip_logsPK, tableName=batch_job_skip_logs', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-13::jdimayuga (generated)
ALTER TABLE dataflow.batch_job_addtl_params ADD CONSTRAINT uk_batch_job_addtl_params UNIQUE (run_id, key_name);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-13', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 13, '7:5fffd2096e468f1d7395593419089cf8', 'addUniqueConstraint constraintName=uk_batch_job_addtl_params, tableName=batch_job_addtl_params', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Changeset src/main/resources/liquibase/2.0.0/schema/changelog.groovy::1498377817196-14::jdimayuga (generated)
ALTER TABLE dataflow.batch_job_adj_run ADD CONSTRAINT uk_batch_job_adj_run UNIQUE (group_id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1498377817196-14', 'jdimayuga (generated)', 'src/main/resources/liquibase/2.0.0/schema/changelog.groovy', NOW(), 14, '7:898e10bbfd705a2744312b8d9ee2fa55', 'addUniqueConstraint constraintName=uk_batch_job_adj_run, tableName=batch_job_adj_run', '', 'EXECUTED', NULL, NULL, '3.5.3', '8377937232');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

