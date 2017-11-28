databaseChangeLog {

  changeSet(id: '1511840279004-3', author: 'dmendoza (generated)') {
    createIndex(indexName: 'STEP_ID_IDX', tableName: 'batch_job_skip_logs') {
      column(name: 'step_id')
    }
  }

}
