databaseChangeLog {
  changeSet(id: '1529561672897-1', author: 'dmendoza (generated)') {
    addColumn(tableName: 'SETTLEMENT_JOB_LOCK') {
      column(name: 'region_group', type: 'varchar(255)')
    }
  }

  changeSet(id: '1529561672897-2', author: 'dmendoza (generated)') {
    dropUniqueConstraint(constraintName: 'UK_STL_JOB_LOCK', tableName: 'SETTLEMENT_JOB_LOCK')
    addUniqueConstraint(columnNames: 'group_id, process_type, stl_calculation_type, region_group', constraintName: 'UK_STL_JOB_LOCK', tableName: 'SETTLEMENT_JOB_LOCK')
  }

}
