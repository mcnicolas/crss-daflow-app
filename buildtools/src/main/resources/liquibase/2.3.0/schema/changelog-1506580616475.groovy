databaseChangeLog {
  changeSet(id: '1506580618314-1', author: 'dmendoza (generated)') {
    createSequence(sequenceName: 'batch_job_queue_seq')
  }

  changeSet(id: '1506580618314-3', author: 'dmendoza (generated)') {
    createTable(tableName: 'batch_job_queue') {
      column(name: 'id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'details', type: 'VARCHAR(20000)')
      column(name: 'job_exec_end', type: 'TIMESTAMP WITHOUT TIME ZONE')
      column(name: 'job_exec_start', type: 'TIMESTAMP WITHOUT TIME ZONE')
      column(name: 'job_execution_id', type: 'BIGINT')
      column(name: 'job_name', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'module', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'queue_date', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'run_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'job_process', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'status', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'task_obj', type: 'VARCHAR(20000)') {
        constraints(nullable: false)
      }
      column(name: 'user_name', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1506580618314-5', author: 'dmendoza (generated)') {
    addPrimaryKey(columnNames: 'id', constraintName: 'batch_job_queuePK', tableName: 'batch_job_queue')
  }

  changeSet(id: '1506580618314-6', author: 'dmendoza (generated)') {
    addUniqueConstraint(columnNames: 'run_id', constraintName: 'UC_BATCH_JOB_QUEUERUN_ID_COL', tableName: 'batch_job_queue')
  }

}
