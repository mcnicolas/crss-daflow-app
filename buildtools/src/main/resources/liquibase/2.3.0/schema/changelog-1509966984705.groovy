databaseChangeLog {

  changeSet(id: '1509966985983-2', author: 'dmendoza (generated)') {
    addColumn(tableName: 'batch_job_queue') {
      column(name: 'starting_date', type: 'TIMESTAMP WITHOUT TIME ZONE')
    }
  }

}
