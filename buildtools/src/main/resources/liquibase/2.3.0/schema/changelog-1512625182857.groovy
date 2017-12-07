databaseChangeLog {
  changeSet(id: '1512625185875-3', author: 'dmendoza (generated)') {
    dropTable(tableName: 'latest_adjustment_lock')
  }

  changeSet(id: '1512625185875-4', author: 'dmendoza (generated)') {
    dropTable(tableName: 'running_adjustment_lock')
  }
}
