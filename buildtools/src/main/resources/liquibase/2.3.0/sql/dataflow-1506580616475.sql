-- *********************************************************************
-- Update Database Script
-- *********************************************************************
-- Change Log: /home/dmendoza/projects/pemc/parent/dataflow-app/buildtools/src/main/resources/liquibase/changelog.groovy
-- Ran at: 9/28/17 2:56 PM
-- Against: postgres@jdbc:postgresql://localhost:5432/crss_test?currentSchema=dataflow
-- Liquibase version: 3.5.3
-- *********************************************************************

-- Lock Database
UPDATE dataflow.databasechangeloglock SET LOCKED = TRUE, LOCKEDBY = '192.168.242.31 (192.168.242.31)', LOCKGRANTED = '2017-09-28 14:56:01.691' WHERE ID = 1 AND LOCKED = FALSE;

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy::1506580618314-1::dmendoza (generated)
CREATE SEQUENCE dataflow.batch_job_queue_seq;

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1506580618314-1', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy', NOW(), 24, '7:64ef75fff19bc27f6c19fcb6881dd054', 'createSequence sequenceName=batch_job_queue_seq', '', 'EXECUTED', NULL, NULL, '3.5.3', '6581761890');

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy::1506580618314-3::dmendoza (generated)
CREATE TABLE dataflow.batch_job_queue (id BIGINT NOT NULL, details VARCHAR(20000), job_exec_end TIMESTAMP WITHOUT TIME ZONE, job_exec_start TIMESTAMP WITHOUT TIME ZONE, job_execution_id BIGINT, job_name VARCHAR(255) NOT NULL, module VARCHAR(255) NOT NULL, queue_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, run_id BIGINT NOT NULL, job_process VARCHAR(255) NOT NULL, status VARCHAR(255) NOT NULL, task_obj VARCHAR(20000) NOT NULL, "user" VARCHAR(255));

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1506580618314-3', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy', NOW(), 25, '7:22791ad4e71ada63f657470a8b15fdf2', 'createTable tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '6581761890');

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy::1506580618314-5::dmendoza (generated)
ALTER TABLE dataflow.batch_job_queue ADD CONSTRAINT batch_job_queuePK PRIMARY KEY (id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1506580618314-5', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy', NOW(), 26, '7:101c149d6dffbe2d6b6c36540179376d', 'addPrimaryKey constraintName=batch_job_queuePK, tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '6581761890');

-- Changeset src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy::1506580618314-6::dmendoza (generated)
ALTER TABLE dataflow.batch_job_queue ADD CONSTRAINT UC_BATCH_JOB_QUEUERUN_ID_COL UNIQUE (run_id);

INSERT INTO dataflow.databasechangelog (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID) VALUES ('1506580618314-6', 'dmendoza (generated)', 'src/main/resources/liquibase/2.3.0/schema/changelog-1506580616475.groovy', NOW(), 27, '7:f085d5ae735515867898310da4ae5218', 'addUniqueConstraint constraintName=UC_BATCH_JOB_QUEUERUN_ID_COL, tableName=batch_job_queue', '', 'EXECUTED', NULL, NULL, '3.5.3', '6581761890');

-- Release Database Lock
UPDATE dataflow.databasechangeloglock SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1;

