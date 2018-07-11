databaseChangeLog {

  changeSet(id: '1531185962837-1', author: 'dmendoza (generated)') {
    addColumn(tableName: 'batch_job_queue') {
      column(name: 'group_id', type: 'varchar(255)')
      column(name: 'region_group', type: 'varchar(255)')
    }
  }

  changeSet(id: '1531185962837-2', author: 'dmendoza (generated)') {
    addColumn(tableName: 'batch_job_queue') {
      column(name: 'trading_date', type: 'TIMESTAMP WITHOUT TIME ZONE')
    }
  }

}
