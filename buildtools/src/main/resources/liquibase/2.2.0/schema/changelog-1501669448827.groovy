databaseChangeLog {
  changeSet(id: '1501669449638-1', author: 'dmendoza (generated)') {
    createTable(tableName: 'SETTLEMENT_JOB_LOCK') {
      column(name: 'id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'created_datetime', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'end_date', type: 'TIMESTAMP(6) WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'group_id', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'locked', type: 'BOOLEAN') {
        constraints(nullable: false)
      }
      column(name: 'parent_job_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'process_type', type: 'VARCHAR(255)')
      column(name: 'start_date', type: 'TIMESTAMP(6) WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1501669449638-2', author: 'dmendoza (generated)') {
    addPrimaryKey(columnNames: 'id', constraintName: 'SETTLEMENT_JOB_LOCKPK', tableName: 'SETTLEMENT_JOB_LOCK')
  }

  changeSet(id: '1501669449638-3', author: 'dmendoza (generated)') {
    addUniqueConstraint(columnNames: 'parent_job_id, group_id', constraintName: 'UK_STL_JOB_LOCK', tableName: 'SETTLEMENT_JOB_LOCK')
  }

}
