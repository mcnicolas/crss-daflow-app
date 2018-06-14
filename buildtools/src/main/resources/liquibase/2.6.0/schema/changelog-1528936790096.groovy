databaseChangeLog {

  changeSet(id: '1528936792820-1', author: 'dmendoza (generated)') {
    addColumn(tableName: 'batch_job_queue') {
      column(name: 'meter_process_type', type: 'varchar(255)')
    }
  }

}
