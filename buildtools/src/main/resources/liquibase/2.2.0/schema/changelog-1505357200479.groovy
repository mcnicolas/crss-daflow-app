databaseChangeLog {
  changeSet(id: '1505357205385-1', author: 'dmendoza (generated)') {
    createIndex(indexName: 'IDX_BATCH_JOB_SKIP_LOGS', tableName: 'BATCH_JOB_SKIP_LOGS') {
      column(name: 'JOB_EXECUTION_ID')
      column(name: 'PARENT_STEP_NAME')
    }
  }

}
